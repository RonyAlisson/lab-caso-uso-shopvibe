# ShopVibe — Laboratório de Engenharia de Dados

Pipeline de **CDC** com **arquitetura medalhão**. Diagrama completo em [docs/arquitetura.drawio](docs/arquitetura.drawio).

```
Postgres ──▶ Debezium ──▶ Kafka ──▶ S3 Sink ──▶ MinIO bronze ──▶ silver ──▶ gold
(orders,      (Kafka Connect)          (Kafka Connect)            (jobs Python, pendente)
 order_items)
```

## Status do projeto

| Componente | Status | Acesso | Login (`.env`) |
|---|---|---|---|
| Postgres | ✅ | `localhost:5432` | `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| pgAdmin | ✅ | http://localhost:8091 | `PGADMIN_EMAIL` / `PGADMIN_PASSWORD` |
| MinIO (buckets `bronze`, `silver`, `gold`) | ✅ | http://localhost:9001 | `MINIO_USER` / `MINIO_PASSWORD` |
| Kafka + Zookeeper | ✅ | `localhost:9092` | — |
| Kafka Connect | ✅ | http://localhost:8083 | — |
| AKHQ (Kafka UI) | ✅ | http://localhost:8080 | — |
| Conector Debezium `connector-postgres` | ✅ Registrado | — | — |
| Conector S3 Sink `connector-minio` | ✅ Registrado | — | — |
| Teste ponta a ponta (INSERT → MinIO) | ⏳ Em andamento | — | — |
| Jobs Python (bronze → silver → gold) | ⏳ Pendente | — | — |

---

## 1. Subir e parar

```powershell
docker compose up -d        # sobe tudo
docker compose ps -a        # postgres/zookeeper/kafka-broker/connect = healthy; mc = Exited (0)
docker compose down         # para (mantém os dados)
```

O `connect` leva 1 a 2 minutos para ficar `healthy`, e o AKHQ só sobe depois dele.

> ⚠️ **`docker compose down -v` apaga tudo** (banco, Kafka, MinIO). Depois dele, refaça os passos 2 e 3.

## 2. Access key do MinIO (manual, uma vez)

O S3 Sink usa `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`. Essa chave **não é criada automaticamente**.

1. Abra http://localhost:9001 e entre com `MINIO_USER` / `MINIO_PASSWORD`.
2. Vá em **Access Keys → Create access key**.
3. Digite os **mesmos valores** de `MINIO_ACCESS_KEY` e `MINIO_SECRET_KEY` do `.env`.

## 3. Registrar os conectores (uma vez)

A configuração fica salva no Kafka (`connect-configs`) e volta sozinha a cada `up`. Ordem: Debezium primeiro, depois o S3 Sink.

```powershell
docker compose exec connect curl -s -X PUT -H "Content-Type: application/json" -d "@/conectores/conector-postgres.json" http://localhost:8083/connectors/connector-postgres/config
docker compose exec connect curl -s -X PUT -H "Content-Type: application/json" -d "@/conectores/conector-minio.json" http://localhost:8083/connectors/connector-minio/config
```

---

## 4. Testes rápidos por ferramenta

| Ferramenta | Comando | Esperado |
|---|---|---|
| Postgres | `docker compose exec postgres psql -U shopvibe -d shopvibe -c "SHOW wal_level;"` | `logical` |
| Postgres | `docker compose exec postgres psql -U shopvibe -d shopvibe -c "\dt public.*"` | `orders`, `order_items` |
| Postgres | `docker compose exec postgres psql -U shopvibe -d shopvibe -c "SELECT slot_name, active FROM pg_replication_slots;"` | `debezium \| t` |
| Zookeeper | `docker compose exec zookeeper bash -c "echo ruok \| nc localhost 2181"` | `imok` |
| Kafka | `docker compose exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list` | `connect-*` e, após INSERTs, `postgres.public.*` |
| Connect | `docker compose exec connect curl -s http://localhost:8083/connectors/connector-postgres/status` | `RUNNING` |
| Connect | `docker compose exec connect curl -s http://localhost:8083/connectors/connector-minio/status` | `RUNNING` |
| MinIO | `docker compose exec minio sh -c 'mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc ls local'` | `bronze/`, `gold/`, `silver/` |

## 5. Teste do pipeline ponta a ponta

**1. Abra o AKHQ** (http://localhost:8080) em **Live Tail**.

**2. Rode no pgAdmin** (**Query Tool** no banco `shopvibe`):

```sql
-- 3 pedidos -> 3 eventos op "c"
INSERT INTO orders (customer_id, total) VALUES (101, 150.00), (102, 89.90), (103, 320.50);

-- 4 itens -> 4 eventos op "c"
INSERT INTO order_items (order_id, product_id, quantity, price)
SELECT o.order_id, v.product_id, v.quantity, v.price
FROM orders o
JOIN (VALUES (101,10,2,75.00), (102,20,1,89.90), (103,30,1,200.50), (103,31,2,60.00))
  AS v(customer_id, product_id, quantity, price) USING (customer_id);

-- evento op "u" (before = CREATED, after = PAID)
UPDATE orders SET status = 'PAID' WHERE customer_id = 101;

-- evento op "d" + tombstone
DELETE FROM order_items WHERE product_id = 31;
```

**3. Confira:**

| Onde | Esperado |
|---|---|
| AKHQ → Topics | `postgres.public.orders` e `postgres.public.order_items` |
| AKHQ → Consumer Groups | `connect-connector-minio` com lag ≈ 0 |
| MinIO → bronze → `topics/` | 1 arquivo `.json` por tópico (o S3 Sink grava a cada **3** mensagens) |
| Conteúdo do arquivo | `"total": "150.00"` (texto legível, não `"Ok4="`) |

Ou pelo terminal:

```powershell
docker compose exec minio sh -c 'mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc ls --recursive local/bronze'
```

---

## 6. Problemas comuns

| Sintoma | Solução |
|---|---|
| Conector `FAILED` | Ver o erro em `docker compose logs connect` ou no status do conector |
| S3 Sink: *"Access Key Id … does not exist"* | Criar a access key no MinIO (passo 2) |
| Debezium: *"password authentication failed"* | O `${env:...}` do JSON precisa existir no `environment:` do `connect` |
| Tópicos `postgres.public.*` não aparecem | Normal até o primeiro INSERT (as tabelas começam vazias) |
| Nada no bucket `bronze` | É preciso pelo menos 3 mensagens por tópico (`flush.size: 3`) |
| Tabelas não existem | O init só roda com o volume vazio: `docker compose down -v` + `up -d` |
| Aviso *"variable is not set"* | O nome no `${...}` do compose difere do `.env` |
