from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 KafkaConsumer: KafkaConsumer,
                 KafkaProducer: KafkaProducer,
                 DdsRepository: DdsRepository,
                 logger: Logger) -> None:
        self._consumer = KafkaConsumer
        self._producer = KafkaProducer
        self._dds_repository = DdsRepository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        current_dt = datetime.now()
        load_src = "kafka"

        i = 0
        cons = self._consumer.consume()
        while (cons is not None) & (i<self._batch_size):
            payload = cons["payload"]
            self._logger.info(f"{datetime.utcnow()}: OKKK1")
            pld_order = int(payload["id"])
            pld_date = payload["date"]
            pld_cost = payload["cost"]
            pld_payment = payload["payment"]
            pld_status = payload["status"]
            pld_restaurant = payload["restaurant"]
            pld_user = payload["user"]
            pld_products = payload["products"]
            self._logger.info(f"{datetime.utcnow()}: OKKK1.1")
            restaurant_id = pld_restaurant["id"]
            restaurant_name = pld_restaurant["name"]
            self._logger.info(f"{datetime.utcnow()}: OKKK1.2")
            user_id = pld_user["id"]
            user_name = pld_user["name"]
            user_login = pld_user["login"]
            self._logger.info(f"{datetime.utcnow()}: OKKK2")
            self._dds_repository.h_user_insert(user_id, current_dt, load_src)
            self._dds_repository.h_restaurant_insert(restaurant_id, current_dt, load_src)
            self._dds_repository.h_order_insert(pld_order, pld_date, current_dt, load_src)
            self._logger.info(f"{datetime.utcnow()}: OKKK3")
            self._dds_repository.s_order_cost_insert(pld_order, pld_cost, pld_payment, current_dt, load_src)
            self._logger.info(f"{datetime.utcnow()}: OKKK4")
            self._dds_repository.s_order_status_insert(pld_order, pld_status, current_dt, load_src)
            self._logger.info(f"{datetime.utcnow()}: OKKK5")
            self._dds_repository.s_restaurant_names_insert(restaurant_id, restaurant_name, current_dt, load_src)
            self._logger.info(f"{datetime.utcnow()}: OKKK6")
            self._dds_repository.s_user_names_insert(user_id, user_name, user_login, current_dt, load_src)
            self._logger.info(f"{datetime.utcnow()}: OKKK7")
            self._dds_repository.l_order_user_insert(pld_order, user_id, current_dt, load_src)
            self._logger.info(f"{datetime.utcnow()}: OKKK8")
            for product in pld_products:
                product_id = product["id"]
                name = product["name"]
                category = product["category"]
                self._logger.info(f"{datetime.utcnow()}: OKKK9")
                self._dds_repository.h_product_insert(product_id, current_dt, load_src)
                self._dds_repository.h_category_insert(category, current_dt, load_src)
                self._logger.info(f"{datetime.utcnow()}: OKKK10")
                self._dds_repository.s_product_names_insert(product_id, name, current_dt, load_src)
                self._logger.info(f"{datetime.utcnow()}: OKKK11")
                self._dds_repository.l_order_product_insert(pld_order, product_id, current_dt, load_src)
                self._dds_repository.l_product_category_insert(category, product_id, current_dt, load_src)
                self._dds_repository.l_product_restaurant_insert(restaurant_id, product_id, current_dt, load_src)
                self._logger.info(f"{datetime.utcnow()}: OKKK12")
                if pld_status == "CLOSED":
                    string_to_kafka = {'user_id':user_id, 'product_id':product_id, 'category':category}
                    self._producer.produce(string_to_kafka)

            i = i+1
            cons = self._consumer.consume()

        self._logger.info(f"{datetime.utcnow()}: FINISH")
