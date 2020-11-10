pipeline {
    agent {label 'docker-slave-r5.2xlarge'}
    stages {
        stage('Build image') {
            steps {
                script {
                    withCredentials([
                            usernamePassword(credentialsId: 'datascience-harbor', usernameVariable: 'DOCKER_USR', passwordVariable: 'DOCKER_PW')
                        ])
                    {
                    sh "docker login -u '${DOCKER_USR}' -p ${DOCKER_PW} https://harbor.aws.c.dk/"
                    image = docker.build("${JOB_NAME}:${BUILD_ID}")
                    }
                }
            }
        }
        stage('Make ingestion') {
            steps {
                script {
                    withCredentials([
                        usernamePassword(credentialsId: 'nuuday-ai-prod-mysql', usernameVariable: 'MARIADB_USR', passwordVariable: 'MARIADB_PSW')
                    ]) {
                            sh "docker run -t --rm -e 'MARIADB_USR=${MARIADB_USR}' -e 'MARIADB_PSW=${MARIADB_PSW}' \
                                '${JOB_NAME}':'${BUILD_ID}'"
                    }
                }
            }
        }
    }
    post {
        always {
            cleanWs()
        }
    }
}