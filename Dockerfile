FROM debian:jessie
RUN apt-get -y update && \
    apt-get install -y ca-certificates
COPY bin/mongo-to-s3 /usr/bin/mongo-to-s3
COPY bin/sfncli /usr/bin/sfncli
CMD ["/usr/bin/sfncli", "--activityname", "${_DEPLOY_ENV}--${_APP_NAME}", "--region", "us-west-2", "--cloudwatchregion", "us-west-1", "--workername", "MAGIC_ECS_TASK_ARN", "--cmd", "/usr/bin/mongo-to-s3"]
