chmod +x sma_cross.sh &&
mkdir /home/pi/Documents/cronlogs &&
python3 -m venv trader &&
source env/bin/activate &&
pip install -r requirements.txt &&
deactivate