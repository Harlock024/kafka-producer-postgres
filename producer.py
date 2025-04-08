import json
import os
import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import pandas as pd


load_dotenv()


SERVER_BOOTSTRAP = os.getenv("SERVER_BOOTSTRAP")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL")
SASL_MECHANISM = os.getenv("SASL_MECHANISM")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")


app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=SERVER_BOOTSTRAP,
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    sasl_plain_username=SASL_USERNAME,
    sasl_plain_password=SASL_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

hostname = socket.gethostname()

def on_success(metadata):
    print(f"Mensaje enviado a topic '{metadata.topic}' en partición {metadata.partition}, offset {metadata.offset}")

def on_error(e):
    print(f"Error al enviar mensaje: {e}")

@app.route('/trigger-producer', methods=['POST'])
def trigger_producer():
    try:
        # Aquí puedes recibir el archivo o los datos JSON desde el cuerpo de la solicitud
        if 'dataset_url' not in request.json:
            return jsonify({"error": "Falta la URL del dataset"}), 400

        url = request.json['dataset_url']
        df = pd.read_json(url, orient="columns")

        # Enviar los primeros 100 registros a Kafka
        for index, row in df.head(100).iterrows():
            data_dict = row.to_dict()
            future = producer.send(
                "spotify",  # El topic de Kafka
                key=hostname,
                value=data_dict
            )
            future.add_callback(on_success)
            future.add_errback(on_error)

        producer.flush()
        producer.close()

        return jsonify({"message": "Datos enviados a Kafka correctamente"}), 200

    except Exception as e:
        print(f"Error al procesar el dataset: {e}")
        return jsonify({"error": f"Error al procesar el dataset: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
