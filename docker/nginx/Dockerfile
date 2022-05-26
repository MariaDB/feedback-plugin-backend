ARG nginx_version=latest
FROM "nginx:$nginx_version"
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d
