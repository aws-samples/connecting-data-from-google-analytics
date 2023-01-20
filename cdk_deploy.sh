

# python virtual envirnment

python3 -m venv .env
 
source .env/bin/activate

# install cdk
pip install -r requirements.txt

npm install -g aws-cdk
cdk bootstrap

# run synth for cdk nag tests
#cdk synth
cdk deploy