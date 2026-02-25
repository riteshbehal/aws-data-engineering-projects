aws ecr get-login-password \
        --region ap-south-1 | docker login \
        --username AWS \
        --password-stdin 463646775279.dkr.ecr.ap-south-1.amazonaws.com

aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin 463646775279.dkr.ecr.ap-south-1.amazonaws.com

docker build -t mobile-signal-app .

docker run -p 8501:8501 -v ${HOME}/.aws:/root/.aws mobile-signal-app

docker tag mobile-signal-app:latest 463646775279.dkr.ecr.ap-south-1.amazonaws.com/streamlit-app:mobile-signal-app

docker push 463646775279.dkr.ecr.ap-south-1.amazonaws.com/streamlit-app:mobile-signal-app