from lib.pg import PgConnect

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_product_counters_insert(self, user: str, product: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """WITH p as (SELECT i.h_product_pk, n.name 
                                      FROM (SELECT h_product_pk FROM dds.h_product WHERE product_id = %(product)s) as i
                                      JOIN (SELECT h_product_pk, name FROM dds.s_product_names) as n
                                        ON i.h_product_pk = n.h_product_pk),
                              u as (SELECT h_user_pk FROM dds.h_user WHERE user_id = %(user)s),
                              v as (SELECT * FROM u CROSS JOIN p),
                              f as (SELECT v.*, coalesce(c.order_cnt,0)+1 FROM v LEFT JOIN cdm.user_product_counters as c ON v.h_user_pk = c.user_id AND v.h_product_pk = c.product_id)
                         INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt) 
                                                        SELECT * 
                                                          FROM f
                                                   ON CONFLICT (user_id, product_id) DO UPDATE
                                                           SET product_name = EXCLUDED.product_name,
                                                               order_cnt = EXCLUDED.order_cnt
                                                     RETURNING *;"""
                cur.execute(sql, {'user': user, 'product':product})

    def user_category_counters_insert(self, user: str, category: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """WITH p as (SELECT h_category_pk, category_name FROM dds.h_category WHERE category_name = %(category)s),
                              u as (SELECT h_user_pk FROM dds.h_user WHERE user_id = %(user)s),
                              v as (SELECT * FROM u CROSS JOIN p),
                              f as (SELECT v.*, coalesce(c.order_cnt,0)+1 FROM v LEFT JOIN cdm.user_category_counters as c 
                                                                                        ON v.h_user_pk = c.user_id AND v.h_category_pk = c.category_id)
                         INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt) 
                                                        SELECT * 
                                                          FROM f
                                                   ON CONFLICT (user_id, category_id) DO UPDATE
                                                           SET category_name = EXCLUDED.category_name,
                                                               order_cnt = EXCLUDED.order_cnt
                                                     RETURNING *;"""
                cur.execute(sql, {'user': user, 'category':category})