FROM debian:stretch
RUN apt-get -y update && \
    apt-get install -y ca-certificates wget gnupg python python-pip
RUN pip install awscli
RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | apt-key add - && \
    echo "deb http://repo.mongodb.org/apt/debian stretch/mongodb-org/4.2 main" | tee /etc/apt/sources.list.d/mongodb-org-4.2.list && \
    apt-get -y update && \
    apt-get install -y mongodb-org
COPY sis_export.sh /usr/bin/sis_export.sh
COPY bin/sfncli /usr/bin/sfncli
CMD ["/usr/bin/sfncli", "--activityname", "${_DEPLOY_ENV}--${_APP_NAME}", "--region", "us-west-2", "--cloudwatchregion", "us-west-1", "--workername", "MAGIC_ECS_TASK_ARN", "--cmd", "/usr/bin/sis_export.sh"]
