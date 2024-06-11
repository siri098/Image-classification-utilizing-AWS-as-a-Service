from flask import Flask, jsonify, request
from gevent import monkey
from queue_manager import SQSQueueManager

monkey.patch_all()

app = Flask(__name__)

queue_manager = SQSQueueManager()

@app.route('/', methods=['POST'])
def upload_image():
    if 'myfile' not in request.files or request.files['myfile'] == '':
        return jsonify({'error': 'No image provided'}), 400
    image = request.files['myfile']
    filename = image.filename
    messageId = queue_manager.handle_request(image, filename)
    result = queue_manager.get_result(messageId)
    if result is None:
        return "Invalid MessageId", 400
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=False)