chmod +x sma_cross.sh &&
mkdir /home/shane/Documents/cronlogs -p &&
python3 -m venv env &&
source env/bin/activate &&
pip install -r requirements.txt &&
deactivate