-- =========================================================
-- ShopVibe - schema transacional (origem dos eventos)
-- =========================================================

CREATE TABLE IF NOT EXISTS public.orders (
    order_id     BIGSERIAL      PRIMARY KEY,
    customer_id  BIGINT         NOT NULL,
    total        NUMERIC(12,2)  NOT NULL CHECK (total >= 0),
    status       VARCHAR(20)    NOT NULL DEFAULT 'CREATED',
    created_at   TIMESTAMP      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.order_items (
    order_id     BIGINT         NOT NULL REFERENCES public.orders (order_id),
    product_id   BIGINT         NOT NULL,
    quantity     INT            NOT NULL CHECK (quantity > 0),
    price        NUMERIC(12,2)  NOT NULL CHECK (price >= 0),
    PRIMARY KEY (order_id, product_id)
);

-- Consultas por cliente em janela de tempo (regra de fraude, análises)
CREATE INDEX IF NOT EXISTS idx_orders_customer_created
    ON public.orders (customer_id, created_at);

-- REPLICA IDENTITY FULL: o Debezium envia a linha COMPLETA no campo "before"
-- em updates e deletes (o padrão envia só a chave primária).
ALTER TABLE public.orders      REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;