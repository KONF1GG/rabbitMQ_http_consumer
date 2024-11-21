import datetime
import pika
import requests
import json
import hashlib
from dotenv import dotenv_values
import clickhouse_connect
import time

config = dotenv_values('.env')

connection_params = pika.ConnectionParameters(
    host=config.get('RABBIT_HOST'),
    port=int(config.get('RABBIT_PORT', 5672)),
    credentials=pika.PlainCredentials(
        config.get('RABBIT_USER'),
        config.get('RABBIT_PASSWORD')
    )
)

clickhouse_client = clickhouse_connect.get_client(
    host=config.get('CLICKHOUSE_HOST'),
    username=config.get('CLICKHOUSE_USER'),
    password=config.get('CLICKHOUSE_PASSWORD')
)


def generate_message_hash(url, body):
    """Генерация хеша для уникальной идентификации сообщения."""
    message_str = f"{url}:{json.dumps(body)}"
    return hashlib.sha256(message_str.encode()).hexdigest()


def check_message_in_logs(client, message_hash):
    """Проверка, если такое сообщение уже есть в логах ClickHouse за последние 24 часа."""
    query = f"""
    SELECT COUNT(*) 
    FROM rabbitmq.http_post_queue_logs
    WHERE message_hash = '{message_hash}' 
    AND timestamp > now() - INTERVAL 1 DAY
    """
    result = client.command(query)
    return result


def log_to_clickhouse(client, url, message_data, status='success', error=None, message_hash=None):
    """Логирование в ClickHouse."""
    url_str = str(url) if url else ''
    message_data_str = json.dumps(message_data).replace("'", "''")
    status_str = str(status) if status else 'unknown'
    error_str = str(error).replace("'", "''") if error else ''
    timestamp = datetime.datetime.now().isoformat()

    query = f"""
    INSERT INTO rabbitmq.http_post_queue_logs (url, payload, status, error, timestamp, message_hash)
    VALUES ('{url_str}', '{message_data_str}', '{status_str}', '{error_str}', '{timestamp}', '{message_hash}')
    """
    client.command(query)


def send_telegram_message(message):
    """Отправка логов в Telegram."""
    url = f'https://api.telegram.org/bot{config.get('API_TOKEN')}/sendMessage'
    data = {'chat_id': config.get('CHAT_ID'), 'text': message}
    data_leo = {'chat_id': config.get('CHAT_ID_LEO'), 'text': message}
    try:
        response = requests.post(url, data=data)
        if response.status_code != 200:
            print(f"Ошибка отправки уведомления: {response.status_code}, {response.text}")

        response = requests.post(url, data=data_leo)
        if response.status_code != 200:
            print(f"Ошибка отправки уведомления: {response.status_code}, {response.text}")

    except Exception as e:
        print(f"Ошибка при отправке уведомления: {e}")


def process_message(ch, method, properties, body):
    """Обработка сообщения из очереди."""
    # Parse the message body
    message = json.loads(body)
    url = message.get('url')
    payload = message.get('body')
    message_hash = generate_message_hash(url, payload)

    try:

        if not url or not payload:
            log_to_clickhouse(clickhouse_client, url, payload, status="failed", error="Сообщение должно содержать "
                                                                                      "поля url и body")
        else:

            # Проверка, если сообщение с таким хешем уже есть в логах за последние 24 часа
            # Send HTTP POST request
            response = requests.post(url, json=payload)

            if response.status_code == 200:
                print(f"Message successfully sent to {url}: {response.status_code}")
                log_to_clickhouse(clickhouse_client, url, payload, status="success", message_hash=message_hash)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                print(f"Failed to send message to {url}. Response code: {response.status_code}")
                # Логируем ошибку и отправляем уведомление в Telegram
                error_message = f"Ошибка: {response.status_code} при попытке отправки запроса на {url}"
                log_to_clickhouse(clickhouse_client, url, payload, status="failed", error=error_message,
                                  message_hash=message_hash)
                message_attempts = check_message_in_logs(clickhouse_client, message_hash)
                if message_attempts > 3:
                    error_message_for_telegram = (f"RabbitMQ ERROR: http_post_queue\n\n"
                                                  f"URL: {url}\n"
                                                  f"Тело запроса: {json.dumps(payload)}\n\n"
                                                  f"Статус ответа от сервера: {response.status_code}\n"
                                                  f"Детали ответа: {response.text}\n\n"
                                                  f"Количество попыток отправки: {message_attempts}\n\n"
                                                  f"hash ошибки: {message_hash}")
                    send_telegram_message(error_message_for_telegram)


    except Exception as e:
        print(f"Error processing message: {e}")
        log_to_clickhouse(clickhouse_client, url, payload, status="failed", error=e,
                          message_hash=message_hash)
        message_attempts = check_message_in_logs(clickhouse_client, message_hash)
        if message_attempts > 3:
            error_message_for_telegram = (f"RabbitMQ ERROR: http_post_queue\n\n"
                                          f"URL: {url}\n"
                                          f"Тело запроса: {json.dumps(payload)}\n\n"
                                          f"Детали ошибки: {e}\n\n"
                                          f"Количество попыток отправки: {message_attempts}\n\n"
                                          f"hash ошибки: {message_hash}")
            send_telegram_message(error_message_for_telegram)
            if message_attempts < 5:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def consumer():
    """Основной потребитель сообщений с переподключением через 10 минут."""
    with pika.BlockingConnection(connection_params) as conn:
        with conn.channel() as ch:
            args = {"x-message-ttl": 86400000}  # TTL 24 часа
            ch.queue_declare(queue='http_post_queue', arguments=args)

            ch.basic_consume(
                queue='http_post_queue',
                on_message_callback=process_message,
            )

            print("Waiting for messages. To exit, press CTRL+C")
            start_time = time.time()
            while True:
                ch.connection.process_data_events(time_limit=10)
                if time.time() - start_time > 600:
                    print("No messages in the last 10 minutes. Reconnecting...")
                    break
            ch.stop_consuming()


if __name__ == '__main__':
    while True:
        consumer()
