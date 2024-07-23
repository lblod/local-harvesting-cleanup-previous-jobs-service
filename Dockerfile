FROM semtech/mu-javascript-template:feature-node-20-upgrade
LABEL maintainer="madnificent@gmail.com"
ENV SUDO_QUERY_RETRY="true"
ENV SUDO_QUERY_RETRY_FOR_HTTP_STATUS_CODES="404,500,503"
