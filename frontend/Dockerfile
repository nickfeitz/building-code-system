# syntax=docker/dockerfile:1.6

# ---- build stage --------------------------------------------------------
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
# package-lock.json may not exist yet on a fresh checkout — fall back to npm install
RUN if [ -f package-lock.json ]; then npm ci; else npm install; fi
COPY . .
RUN npm run build

# ---- runtime stage ------------------------------------------------------
FROM nginx:1.25-alpine

RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=5s \
    CMD wget --quiet --tries=1 --spider http://localhost/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
