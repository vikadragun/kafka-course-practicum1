from confluent_kafka import Producer
from message import Message
import time
from confluent_kafka.serialization import StringSerializer, SerializationError


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_producer():
    # конфиг для продюсера
    producer = Producer(
        {'bootstrap.servers': 'localhost:9095', # адрес Kafka-сервера
         'acks' : '1', #количество реплик, которые должны подтвердить синхронизацию
         'retries': 3 # Количество попыток при сбоях
        }
    )
    return producer

def send_messages(producer, message):

    string_serializer = StringSerializer('utf-8') # инициализация сериализатора сообщения
    value = ''
    while True:
        try:
            value = string_serializer(message) # попытка сериализации
        except SerializationError as e:
            print(f'SerializationError {str(e)}') # вывод ошибки сериализации
            continue

        producer.produce('first-topic', value=value, callback=delivery_report)  # отправка сообщения в топик
        producer.poll(0)  # обработка событий, чтобы вызвать колбэк доставки
        print(f"Sent: {message}") # вывод текста сообщения
        time.sleep(1)  # задержка между отправками

if __name__ == "__main__":
    producer = create_producer()
    message = Message('message','Это сообщение отправлено через Кафку') #создание и инициализация экземпляра Message
    print(message)
    try:
        send_messages(producer,message.create_message())
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()  # ожидание завершения всех сообщений
