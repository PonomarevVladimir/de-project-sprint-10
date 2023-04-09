import uuid
from datetime import datetime

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, user_id: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src) 
                                         VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s) 
                                    ON CONFLICT (user_id) DO UPDATE
                                            SET load_dt = EXCLUDED.load_dt,
                                                load_src = EXCLUDED.load_src
                                      RETURNING *;"""
                cur.execute(sql, {'h_user_pk': uuid.uuid4(), 'user_id': user_id, 'load_dt': load_dt, 'load_src': load_src})

    def h_restaurant_insert(self, restaurant_id: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src) 
                                               VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s) 
                                          ON CONFLICT (restaurant_id) DO UPDATE
                                                  SET load_dt = EXCLUDED.load_dt,
                                                      load_src = EXCLUDED.load_src
                                            RETURNING *;"""
                cur.execute(sql, {'h_restaurant_pk': uuid.uuid4(), 'restaurant_id': restaurant_id, 'load_dt': load_dt, 'load_src': load_src})
            
    def h_product_insert(self, product_id: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src) 
                                            VALUES (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s) 
                                       ON CONFLICT (product_id) DO UPDATE
                                               SET load_dt = EXCLUDED.load_dt,
                                                   load_src = EXCLUDED.load_src
                                         RETURNING *;"""
                cur.execute(sql, {'h_product_pk': uuid.uuid4(), 'product_id': product_id, 'load_dt': load_dt, 'load_src': load_src})

    def h_category_insert(self, category_name: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src) 
                                             VALUES (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s) 
                                        ON CONFLICT (category_name) DO UPDATE
                                                SET load_dt = EXCLUDED.load_dt,
                                                    load_src = EXCLUDED.load_src
                                          RETURNING *;"""
                cur.execute(sql, {'h_category_pk': uuid.uuid4(), 'category_name': category_name, 'load_dt': load_dt, 'load_src': load_src})

    def h_order_insert(self, order_id: int, order_dt: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src) 
                                          VALUES (%(h_order_pk)s, %(order_id)s, to_timestamp(%(order_dt)s, 'YYYY-MM-DD hh24:mi:ss'), %(load_dt)s, %(load_src)s) 
                                     ON CONFLICT (order_id) DO UPDATE
                                             SET order_dt = EXCLUDED.order_dt,
                                                 load_dt = EXCLUDED.load_dt,
                                                 load_src = EXCLUDED.load_src
                                       RETURNING *;"""
                cur.execute(sql, {'h_order_pk': uuid.uuid4(), 'order_id': order_id, 'order_dt': order_dt, 'load_dt': load_dt, 'load_src': load_src})

    def l_order_product_insert(self, order_id: str, product_id: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src) 
                                                  SELECT %(hk_order_product_pk)s, o.h_order_pk, p.h_product_pk, %(load_dt)s, %(load_src)s
                                                    FROM (SELECT h_order_pk FROM dds.h_order WHERE order_id = %(order_id)s) as o
                                              CROSS JOIN (SELECT h_product_pk FROM dds.h_product WHERE product_id = %(product_id)s) as p
                                             ON CONFLICT (h_order_pk, h_product_pk) DO UPDATE
                                                     SET load_dt = EXCLUDED.load_dt,
                                                         load_src = EXCLUDED.load_src
                                               RETURNING *;"""
                cur.execute(sql, {'hk_order_product_pk': uuid.uuid4(), 'order_id': order_id, 'product_id': product_id, 'load_dt': load_dt, 'load_src': load_src})

    def l_order_user_insert(self, order_id: str, user_id: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src) 
                                               SELECT %(hk_order_user_pk)s, o.h_order_pk, u.h_user_pk, %(load_dt)s, %(load_src)s
                                                 FROM (SELECT h_order_pk FROM dds.h_order WHERE order_id = %(order_id)s) as o
                                           CROSS JOIN (SELECT h_user_pk FROM dds.h_user WHERE user_id = %(user_id)s) as u
                                          ON CONFLICT (h_order_pk, h_user_pk) DO UPDATE
                                                  SET load_dt = EXCLUDED.load_dt,
                                                      load_src = EXCLUDED.load_src
                                            RETURNING *;"""
                cur.execute(sql, {'hk_order_user_pk': uuid.uuid4(), 'order_id': order_id, 'user_id': user_id, 'load_dt': load_dt, 'load_src': load_src})

    def l_product_category_insert(self, category_name: str, product_id: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src) 
                                                     SELECT %(hk_product_category_pk)s, c.h_category_pk, p.h_product_pk, %(load_dt)s, %(load_src)s
                                                       FROM (SELECT h_category_pk FROM dds.h_category WHERE category_name = %(category_name)s) as c
                                                 CROSS JOIN (SELECT h_product_pk FROM dds.h_product WHERE product_id = %(product_id)s) as p
                                                ON CONFLICT (h_category_pk, h_product_pk) DO UPDATE
                                                        SET load_dt = EXCLUDED.load_dt,
                                                            load_src = EXCLUDED.load_src
                                                  RETURNING *;"""
                cur.execute(sql, {'hk_product_category_pk': uuid.uuid4(), 'category_name': category_name, 'product_id': product_id, 'load_dt': load_dt, 'load_src': load_src})

    def l_product_restaurant_insert(self, restaurant_id: str, product_id: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src) 
                                                       SELECT %(hk_product_restaurant_pk)s, r.h_restaurant_pk, p.h_product_pk, %(load_dt)s, %(load_src)s
                                                         FROM (SELECT h_restaurant_pk FROM dds.h_restaurant WHERE restaurant_id = %(restaurant_id)s) as r
                                                   CROSS JOIN (SELECT h_product_pk FROM dds.h_product WHERE product_id = %(product_id)s) as p
                                                  ON CONFLICT (h_restaurant_pk, h_product_pk) DO UPDATE
                                                          SET load_dt = EXCLUDED.load_dt,
                                                              load_src = EXCLUDED.load_src
                                                    RETURNING *;"""
                cur.execute(sql, {'hk_product_restaurant_pk': uuid.uuid4(), 'restaurant_id': restaurant_id, 'product_id': product_id, 'load_dt': load_dt, 'load_src': load_src})

    def s_order_cost_insert(self, order_id: str, cost, payment, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.s_order_cost (hk_order_cost_pk, h_order_pk, cost, payment, load_dt, load_src) 
                                               SELECT %(hk_order_cost_pk)s, a.h_order_pk, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s
                                                 FROM (SELECT h_order_pk FROM dds.h_order WHERE order_id = %(order_id)s) as a
                                    ON CONFLICT (h_order_pk) DO UPDATE
                                            SET cost = EXCLUDED.cost,
                                                payment = EXCLUDED.payment,
                                                load_dt = EXCLUDED.load_dt,
                                                load_src = EXCLUDED.load_src
                                      RETURNING *;"""
                cur.execute(sql, {'hk_order_cost_pk': uuid.uuid4(), 'order_id': order_id, 'cost': cost, 'payment': payment, 'load_dt': load_dt, 'load_src': load_src})

    def s_order_status_insert(self, order_id: str, status: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.s_order_status (hk_order_status_pk, h_order_pk, status, load_dt, load_src) 
                                                 SELECT %(hk_order_status_pk)s, a.h_order_pk, %(status)s, %(load_dt)s, %(load_src)s
                                                   FROM (SELECT h_order_pk FROM dds.h_order WHERE order_id = %(order_id)s) as a
                                            ON CONFLICT (h_order_pk) DO UPDATE
                                                    SET status = EXCLUDED.status,
                                                        load_dt = EXCLUDED.load_dt,
                                                        load_src = EXCLUDED.load_src
                                              RETURNING *;"""
                cur.execute(sql, {'hk_order_status_pk': uuid.uuid4(), 'order_id': order_id, 'status': status, 'load_dt': load_dt, 'load_src': load_src})

    def s_product_names_insert(self, product_id: str, name: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.s_product_names (hk_product_names_pk, h_product_pk, name, load_dt, load_src) 
                                                  SELECT %(hk_product_names_pk)s, a.h_product_pk, %(name)s, %(load_dt)s, %(load_src)s
                                                    FROM (SELECT h_product_pk FROM dds.h_product WHERE product_id = %(product_id)s) as a
                                             ON CONFLICT (h_product_pk) DO UPDATE
                                                     SET name = EXCLUDED.name,
                                                         load_dt = EXCLUDED.load_dt,
                                                         load_src = EXCLUDED.load_src
                                               RETURNING *;"""
                cur.execute(sql, {'hk_product_names_pk': uuid.uuid4(), 'product_id': product_id, 'name': name, 'load_dt': load_dt, 'load_src': load_src})

    def s_restaurant_names_insert(self, restaurant_id: str, name: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.s_restaurant_names (hk_restaurant_names_pk, h_restaurant_pk, name, load_dt, load_src) 
                                                  SELECT %(hk_restaurant_names_pk)s, a.h_restaurant_pk, %(name)s, %(load_dt)s, %(load_src)s
                                                    FROM (SELECT h_restaurant_pk FROM dds.h_restaurant WHERE restaurant_id = %(restaurant_id)s) as a
                                             ON CONFLICT (h_restaurant_pk) DO UPDATE
                                                     SET name = EXCLUDED.name,
                                                         load_dt = EXCLUDED.load_dt,
                                                         load_src = EXCLUDED.load_src
                                               RETURNING *;"""
                cur.execute(sql, {'hk_restaurant_names_pk': uuid.uuid4(), 'restaurant_id': restaurant_id, 'name': name, 'load_dt': load_dt, 'load_src': load_src})

    def s_user_names_insert(self, restaurant_id: str, username: str, userlogin: str, load_dt: datetime, load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                sql = """INSERT INTO dds.s_user_names (hk_user_names_pk, h_user_pk, username, userlogin, load_dt, load_src) 
                                               SELECT %(hk_user_names_pk)s, a.h_user_pk, %(name)s, %(login)s, %(load_dt)s, %(load_src)s
                                                 FROM (SELECT h_user_pk FROM dds.h_user WHERE user_id = %(user_id)s) as a
                                          ON CONFLICT (h_user_pk) DO UPDATE
                                                  SET username = EXCLUDED.username,
                                                      userlogin = EXCLUDED.userlogin,
                                                      load_dt = EXCLUDED.load_dt,
                                                      load_src = EXCLUDED.load_src
                                            RETURNING *;"""
                cur.execute(sql, {'hk_user_names_pk': uuid.uuid4(), 'user_id': restaurant_id, 'name': username, 'login': userlogin, 'load_dt': load_dt, 'load_src': load_src})
