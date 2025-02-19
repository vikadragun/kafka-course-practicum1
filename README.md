Проект состоит из четрех файлов: message.py, producer.py, consumer_pull.py и consumer_push.py.
В файле message.py описан класс Message(), в качестве параметров принимает 2 строки. 
Для начала работы необходимо запустить кафку через докер и с помощью команды kafka-topics.sh --create --topic first-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
создать топик.
Далее указать название топика и адрес Kafka-сервера в файлах проекта. 
Проверить работоспособность можно, запустив файлы producer.py, consumer_pull.py и consumer_push.py.
