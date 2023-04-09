from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 KafkaConsumer: KafkaConsumer,
                 KafkaProducer: KafkaProducer,
                 CdmRepository: CdmRepository,
                 logger: Logger) -> None:
        self._consumer = KafkaConsumer
        self._producer = KafkaProducer
        self._cdm_repository = CdmRepository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        i = 0
        cons = self._consumer.consume()
        while (cons is not None) & (i<self._batch_size):
            user_id = cons["user_id"]
            product_id = cons["product_id"]
            category = cons["category"]
            self._logger.info(f"{datetime.utcnow()}: OKKK1")
            self._cdm_repository.user_product_counters_insert(user_id, product_id)
            self._logger.info(f"{datetime.utcnow()}: OKKK2")
            self._cdm_repository.user_category_counters_insert(user_id, category)
            self._logger.info(f"{datetime.utcnow()}: OKKK3")

            i = i+1
            cons = self._consumer.consume()

        self._logger.info(f"{datetime.utcnow()}: FINISH")
