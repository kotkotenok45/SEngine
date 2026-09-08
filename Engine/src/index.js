import dns from 'node:dns/promises';

const DOMAINS_TXT_URL = "https://githubusercontent.com";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const query = url.searchParams.get("q") || "";
    const cleanQuery = query.toLowerCase().trim();

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Content-Type": "application/json;charset=UTF-8"
    };

    if (request.method === "OPTIONS") return new Response(null, { headers: corsHeaders });
    if (!cleanQuery) return new Response(JSON.stringify([]), { headers: corsHeaders });

    let response;
    try {
      // ЭТАП 1: Скачивание текстовой БД доменов
      response = await fetch(DOMAINS_TXT_URL);
    } catch (fetchErr) {
      return new Response(JSON.stringify({ 
        error: "ОШИБКА БАЗЫ ДАННЫХ: Не удалось скачать файл доменов с GitHub. Проверьте интернет-соединение воркера или доступность URL.",
        details: fetchErr.message 
      }), { headers: corsHeaders, status: 500 });
    }

    if (!response.ok) {
      return new Response(JSON.stringify({ 
        error: `ОШИБКА БАЗЫ ДАННЫХ: Сервер GitHub вернул статус ${response.status} вместо файла доменов.`
      }), { headers: corsHeaders, status: 500 });
    }

    try {
      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      
      let finalResults = [];
      let currentLine = "";
      let done = false;

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

      // ЭТАП 2: Работа асинхронного DNS конвейера
      async function checkWorker() {
        while (finalResults.length < 20) {
          const targetUrl = await getNextMatchedUrl();
          if (!targetUrl) break;

          try {
            const parsedUrl = new URL(targetUrl);
            const hostname = parsedUrl.hostname;

            // Проверка работоспособности DNS модуля
            let ipAddresses = [];
            try {
              ipAddresses = await dns.resolve(hostname);
            } catch (dnsErr) {
              // Если это системная ошибка самого воркера (модуль не поддерживается)
              if (dnsErr.message.includes("not implemented") || dnsErr.message.includes("undefined")) {
                throw new Error(`ОШИБКА ОКРУЖЕНИЯ WORKER: Модуль 'node:dns' заблокирован или не поддерживается. Проверьте наличие флага 'nodejs_compat' в wrangler.json. Внутренний текст: ${dnsErr.message}`);
              }
              // Обычные ошибки ненайденных доменов (NXDOMAIN) просто пропускаем
              continue;
            }
            
            if (ipAddresses.length > 0) {
              finalResults.push({
                url: targetUrl,
                domain: hostname,
                ip: ipAddresses[0] || "Unknown",
                title: hostname
              });
            }
          } catch (e) {
            // Передаем критическую ошибку модуля наверх, остальные гасим
            if (e.message.includes("ОШИБКА ОКРУЖЕНИЯ WORKER")) throw e;
          }
        }
      }

      // Запуск 4 параллельных потоков
      await Promise.all([checkWorker(), checkWorker(), checkWorker(), checkWorker()]);

      if (!done) await reader.cancel();

      return new Response(JSON.stringify(finalResults), { headers: corsHeaders });

    } catch (err) {
      // Сюда прилетают ошибки парсинга или падения критических модулей воркера
      return new Response(JSON.stringify({ 
        error: "ОШИБКА ВНУТРЕННЕЙ ЛОГИКИ WORKER (Критический сбой кода)", 
        details: err.message 
      }), { headers: corsHeaders, status: 500 });
    }
  }
};
