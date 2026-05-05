import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 透過 Dashboard 反向代理存取時，資源需帶 /app/db-operating/ 前綴
export default defineConfig({
  plugins: [react()],
  base: '/app/db-operating/',
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
})
