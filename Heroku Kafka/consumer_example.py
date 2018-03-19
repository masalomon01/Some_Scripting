import config
from heroku_kafka_eze import HerokuSSL
from heroku_kafka_eze import HerokuKafkaConsumer

"""
All the variable names here will be received from Heroku api
*topics are optional and you can pass as many as you want in for the consumer to track,
however if you want to subscribe after creation just use .subscribe as shown below.

Note: The KAFKA_PREFIX will be added on automatically so don't worry about passing it in.
"""
app_name = config.APP_NAME
secret_key = config.SECRET_KEY
heroku_ssl = HerokuSSL(app_name, secret_key)
config = heroku_ssl.get_config()

consumer = HerokuKafkaConsumer(
        'test', # Optional: You don't need to pass any topic at all
        url=config['KAFKA_URL'], # Url string provided by heroku
        ssl_cert=config['KAFKA_CLIENT_CERT'], # Client cert string
        ssl_key=config['KAFKA_CLIENT_CERT_KEY'], # Client cert key string
        ssl_ca=config['KAFKA_TRUSTED_CERT'], # Client trusted cert string
        prefix=config['KAFKA_PREFIX'] # Prefix provided by heroku
    )

"""
To subscribe to topic(s) after creating a consumer pass in a list of topics without the
KAFKA_PREFIX.
"""
consumer.subscribe(topics=('test'))

"""
.assign requires a full topic name with prefix
"""
# consumer.assign([TopicPartition('topic_with_prefix', 2)])

"""
Listening to events it is exactly the same as in kafka_python.
Read the documention linked below for more info!
"""
for msg in consumer:
    print (msg)