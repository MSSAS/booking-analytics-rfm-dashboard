# Сборка Vue
FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build

# Прод-контейнер с nginx
FROM nginx:alpine

# Наш конфиг вместо дефолтного
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Сборка Vue попадает в nginx
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
