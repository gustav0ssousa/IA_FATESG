"""
FinDash — Flask API Server
Personal Financial Dashboard Backend
"""

from flask import Flask, request, jsonify
import database as db

app = Flask(__name__, static_folder='static', static_url_path='')


@app.route('/')
def index():
    return app.send_static_file('index.html')


# ========================
# TRANSACTIONS
# ========================

@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    return jsonify(db.get_transactions(year, month))


@app.route('/api/transactions', methods=['POST'])
def create_transaction():
    return jsonify(db.create_transaction(request.get_json())), 201


@app.route('/api/transactions/<tx_id>', methods=['PUT'])
def update_transaction(tx_id):
    tx = db.update_transaction(tx_id, request.get_json())
    return jsonify(tx) if tx else (jsonify({'error': 'Not found'}), 404)


@app.route('/api/transactions/<tx_id>', methods=['DELETE'])
def delete_transaction(tx_id):
    db.delete_transaction(tx_id)
    return jsonify({'success': True})


# ========================
# RECURRING
# ========================

@app.route('/api/recurring', methods=['GET'])
def get_recurring():
    return jsonify(db.get_recurring())


@app.route('/api/recurring', methods=['POST'])
def create_recurring():
    return jsonify(db.create_recurring(request.get_json())), 201


@app.route('/api/recurring/<rec_id>', methods=['PUT'])
def update_recurring(rec_id):
    rec = db.update_recurring(rec_id, request.get_json())
    return jsonify(rec) if rec else (jsonify({'error': 'Not found'}), 404)


@app.route('/api/recurring/<rec_id>', methods=['DELETE'])
def delete_recurring(rec_id):
    db.delete_recurring(rec_id)
    return jsonify({'success': True})


@app.route('/api/recurring/generate', methods=['POST'])
def generate_recurring():
    data = request.get_json()
    year, month = data.get('year'), data.get('month')
    if not year or not month:
        return jsonify({'error': 'year and month required'}), 400
    generated = db.generate_recurring(year, month)
    return jsonify({'generated': generated, 'count': len(generated)})


# ========================
# GOALS
# ========================

@app.route('/api/goals', methods=['GET'])
def get_goals():
    return jsonify(db.get_goals())


@app.route('/api/goals', methods=['POST'])
def save_goals():
    db.save_goals(request.get_json())
    return jsonify({'success': True})


# ========================
# ANNUAL SUMMARY
# ========================

@app.route('/api/summary/annual', methods=['GET'])
def annual_summary():
    year = request.args.get('year', type=int)
    if not year:
        return jsonify({'error': 'year required'}), 400
    return jsonify(db.get_annual_summary(year))


# ========================
# EXPORT / IMPORT
# ========================

@app.route('/api/export', methods=['GET'])
def export_data():
    return jsonify(db.export_all())


@app.route('/api/import', methods=['POST'])
def import_data():
    count = db.import_all(request.get_json())
    return jsonify({'success': True, 'transaction_count': count})


# ========================
# RUN
# ========================

if __name__ == '__main__':
    db.init_db()
    print("\n  FinDash Server")
    print("  http://localhost:5000\n")
    app.run(debug=True, port=5000, host='0.0.0.0')
