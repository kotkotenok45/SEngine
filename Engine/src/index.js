import dns from 'node:dns/promises';

// Проверенный рабочий URL списка доменов
const DOMAINS_TXT_URL = "https://raw.githubusercontent.com/zer0h/top-1000000-domains/master/top-10000-domains";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const query = url.searchParams.get("q") || "";
    const cleanQuery = query.toLowerCase().trim();

    // Заголовки CORS, чтобы GitHub Pages мог получать данные
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Content-Type": "application/json;charset=UTF-8"
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: corsHeaders });
    if (!cleanQuery) return new Response(JSON.stringify([]), { headers: corsHeaders });

    try {
      // 1. Скачиваем список доменов
      const response = await fetch(DOMAINS_TXT_URL);
      if (!response.body) throw new Error("Не удалось открыть поток данных");

      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      
      let finalResults = [];
      let currentLine = "";
      let done = false;

      // Потоковый поиск совпадений в URL
      async function getNextMatchedUrl() {
        while (!done) {
          const { value, done: streamDone } = await reader.read();
          if (streamDone) {
            done = true;
            if (currentLine.toLowerCase().includes(cleanQuery)) return formatUrl(currentLine);
            break;
          }

          currentLine += decoder.decode(value, { stream: true });
          const lines = currentLine.split("\n");
          currentLine = lines.pop() || "";

          for (const line of lines) {
            if (line.toLowerCase().includes(cleanQuery)) {
              return formatUrl(line);
            }
          }
        }
        return null;
      }

      function formatUrl(line) {
        const clean = line.split(",")[0].trim();
        return clean.startsWith("http") ? clean : `https://${clean}`;
      }

      // Асинхронный воркер: делает только DNS проверку, что гарантирует стабильность
      async function checkWorker() {
        while (finalResults.length < 20) {
          const targetUrl = await getNextMatchedUrl();
          if (!targetUrl) break;

          try {
            const parsedUrl = new URL(targetUrl);
            const hostname = parsedUrl.hostname;

            // Живая проверка через глобальный DNS
            const ipAddresses = await dns.resolve(hostname).catch(() => []);
            
            // Если DNS вернул IP — значит сайт существует в интернетах, добавляем его!
            if (ipAddresses.length > 0) {
              finalResults.push({
                url: targetUrl,
                domain: hostname,
                ip: ipAddresses[0] || "Unknown",
                title: hostname // Используем имя домена в качестве заголовка
              });
            }
          } catch (e) {
            // Игнорируем ошибки конкретного домена
          }
        }
      }

      // Запуск в 4 параллельных потока для максимальной скорости
      await Promise.all([checkWorker(), checkWorker(), checkWorker(), checkWorker()]);

      if (!done) await reader.cancel();

      return new Response(JSON.stringify(finalResults), { headers: corsHeaders });

    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), { headers: corsHeaders, status: 500 });
    }
  }
};
