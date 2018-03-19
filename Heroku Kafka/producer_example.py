import config
from heroku_kafka_eze import HerokuSSL
from heroku_kafka_eze import HerokuKafkaProducer


app_name = config.APP_NAME
secret_key = config.SECRET_KEY
heroku_ssl = HerokuSSL(app_name, secret_key)
config = heroku_ssl.get_config()

"""
All the config variable needed well be received from heroku.
"""

producer = HerokuKafkaProducer(
        url=config['KAFKA_URL'], # Url string provided by heroku
        ssl_cert=config['KAFKA_CLIENT_CERT'], # Client cert string
        ssl_key=config['KAFKA_CLIENT_CERT_KEY'], # Client cert key string
        ssl_ca=config['KAFKA_TRUSTED_CERT'], # Client trusted cert string
        prefix=config['KAFKA_PREFIX'] # Prefix provided by heroku
    )

"""
The .send method will automatically prefix your topic with the KAFKA_PREFIX
NOTE: If the message doesn't seem to be sending try `producer.flush()` to force send.
"""
producer.send('test', b"hola todos")
producer.flush()