docker run --name mysql -d \
    -p 3306:3306 \
    -e MYSQL_ROOT_PASSWORD_FILE=/run/secrets/dev_mysql \
    -v $(echo "$PWD")/secrets:/run/secrets \
    -v mysql:/var/lib/mysql \
    --network finn_scrap \
    --restart unless-stopped \
    mysql:8