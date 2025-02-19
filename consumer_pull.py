from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer, SerializationError

def create_consumer():
    # конфиг консьюмера
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9095', # адрес Kafka-сервера
        'group.id': 'my_group1', # у консьюмеров разные группы, чтобы они имели доступ к одним и тем же сообщениям параллельно
        'auto.offset.reset': 'earliest', # сначала обрабатывать сообщения, которые пришли раньше
        'fetch.min.bytes': 1024, # эта настройка определяет минимальный объём данных (в байтах), который консьюмер должен получить за один запрос к брокеру Kafka
        'enable.auto.commit': False # отключение автокоммита смещений
    })
    consumer.subscribe(['first-topic']) # получать данные из топика "first-topic"
    return consumer


def poll_messages(consumer):
    string_deserializer = StringDeserializer('utf-8') # инициализация десериализатора сообщения
    while True:
        msg = consumer.poll(timeout=1.0)  # проверяем наличие сообщений каждые 1 секунду
        if msg is None: # проверка сообщения на пустоту
            print("Received an empty message.")
            continue
        if msg.error(): # обработка исключений
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Конец раздела {msg.topic()} [{msg.partition()}]')
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            try:
                recieved_message = string_deserializer(msg.value()) # попытка десериализации сообщения
            except SerializationError as e:
                print(f'DeserializationError {str(e)}') # вывод ошибки десериализации
                continue

            print(f"Pull Consumer Received: {recieved_message}") # вывод полученного сообщения
            consumer.commit(asynchronous=False) # смещение коммитов


if __name__ == "__main__":
    consumer = create_consumer()
    try:
        poll_messages(consumer)
    except KeyboardInterrupt:
        print("Stopping pull consumer...")
    finally:
        consumer.close()
