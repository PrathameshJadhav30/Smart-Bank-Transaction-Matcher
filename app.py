import io
import re
import pandas as pd
from flask import Flask, render_template, request, send_file, session, redirect, url_for
from flask_session import Session

app = Flask(__name__)
app.secret_key = 'secret'
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_COOKIE_NAME'] = 'session'
Session(app)

# Function to extract UTR number from any text
def extract_utr(text):
    if isinstance(text, str):
        match = re.search(r'UTR\s*No\.*\s*[:\-]?\s*([A-Z0-9]+)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route('/match', methods=['POST'])
def match():
    hdfc_file = request.files['hdfc_file']
    cbs_file = request.files['cbs_file']

    if hdfc_file and cbs_file:
        hdfc_df = pd.read_excel(hdfc_file)
        cbs_df = pd.read_excel(cbs_file)

        # Clean and standardize HDFC Txn Ref No
        hdfc_df['Txn Ref No'] = hdfc_df['Txn Ref No'].astype(str).str.strip()

        # Combine all columns in CBS for UTR extraction
        cbs_df['Combined'] = cbs_df.fillna('').astype(str).apply(lambda row: ' '.join(row), axis=1)
        cbs_df['Extracted_UTR'] = cbs_df['Combined'].apply(extract_utr)

        # Drop duplicate UTRs to avoid merge conflicts
        cbs_df.drop_duplicates(subset='Extracted_UTR', inplace=True)

        # Perform match
        matched_df = pd.merge(hdfc_df, cbs_df, left_on='Txn Ref No', right_on='Extracted_UTR', how='inner')

        # Get unmatched HDFC entries
        unmatched_df = hdfc_df[~hdfc_df['Txn Ref No'].isin(matched_df['Txn Ref No'])].copy()

        # Prepare unmatched output with only needed columns
        columns_required = ['Txn Ref No', 'Narration', 'Cr Amt']
        unmatched_df = unmatched_df[[col for col in columns_required if col in unmatched_df.columns]]

        # Replace 0 values in Cr Amt with blank
        if 'Cr Amt' in unmatched_df.columns:
            unmatched_df['Cr Amt'] = unmatched_df['Cr Amt'].apply(lambda x: '' if x == 0 else x)

        # Save matched and unmatched to memory
        matched_io = io.BytesIO()
        unmatched_io = io.BytesIO()
        matched_df.to_excel(matched_io, index=False)
        unmatched_df.to_excel(unmatched_io, index=False)
        matched_io.seek(0)
        unmatched_io.seek(0)

        session['matched'] = matched_io.getvalue()
        session['unmatched'] = unmatched_io.getvalue()

        return redirect(url_for('results'))

    return redirect(url_for('index'))

@app.route('/results')
def results():
    if 'matched' in session and 'unmatched' in session:
        return render_template('results.html')
    return redirect(url_for('index'))

@app.route('/download/<filetype>')
def download(filetype):
    if filetype == 'matched' and 'matched' in session:
        return send_file(io.BytesIO(session['matched']), as_attachment=True, download_name='matched.xlsx')
    elif filetype == 'unmatched' and 'unmatched' in session:
        return send_file(io.BytesIO(session['unmatched']), as_attachment=True, download_name='unmatched.xlsx')
    else:
        return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
