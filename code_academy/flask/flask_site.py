from flask import Flask, render_template
from datetime import date
td=date.today()
from dictionary import data

app = Flask(__name__)

#irasau nauja komentara

@app.route('/')
def index():
    return render_template('index.html', data=data)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/services')
def services():
    return render_template('services.html')

if __name__ == '__main__':
  app.run(host='127.0.0.1', port=8000, debug=True)