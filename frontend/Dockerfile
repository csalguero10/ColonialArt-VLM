FROM node:20-slim

WORKDIR /app

# Copiar archivos de dependencias
COPY package*.json ./

# Instalar dependencias
RUN npm install

# Copiar el resto del código
COPY . .

# Exponer el puerto de Vite
EXPOSE 5173

# Ejecutar en modo desarrollo
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]