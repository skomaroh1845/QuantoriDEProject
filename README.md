# QuantoriDEProject
## About 
This pipeline gets top-10 most similar molecules for input set of molecules. Where input set of molecules is taken from AWS S3 bucket. 

## Get started

#### 1) Docker
Make sure that you have docker on your machine. How to install it see [here](https://www.docker.com/get-started/).

#### 2) Clone repo
Open the terminal in the folder where you want to store the project and tipe the following command in the terminal:
```bash
git clone https://github.com/skomaroh1845/QuantoriDEProject.git
```

#### 3) Build the docker image and run the containers 
Run this command in the terminal:
```bash
docker-compose up --build
```
The terminal should be opened in the folder with docker-compose.yml and dockerfile.

#### 4) Open and log in the Airflow UI
Open a tab in your web browser at `http://localhost:8080/`. Login and password are `admin` by default. After logging you should see something similar to this:

![alt text](imgs/image-2.png)

#### 5) Launch the DAG
Click the trigger DAG button on the right side and go drink coffee, now machines is working instead of you.

![alt text](imgs/image-3.png)



## Pipeline structure
![alt text](imgs/image-1.png)


