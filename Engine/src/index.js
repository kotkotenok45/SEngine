import dns from 'node:dns/promises';

const DOMAINS_TXT_URL = "https://raw.githubusercontent.com/zer0h/top-1000000-domains/master/top-10000-domains";

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
      response = await fetch(DOMAINS_TXT_URL);
    } catch (fetchErr) {
      return new Response(JSON.stringify({ 
        error: "ОШИБКА БАЗЫ ДАННЫХ: Не удалось скачать файл доменов с GitHub.",
        details: fetchErr.message 
      }), { headers: corsHeaders, status: 500 });
    }

    if (!response.ok) {
      return new Response(JSON.stringify({ 
        error: `ОШИБКА БАЗЫ ДАННЫХ: Сервер GitHub вернул статус ${response.status}`
      }), { headers: corsHeaders, status: 500 });
    }

    try {
      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      
      let finalResults = [];
      let currentLine = "";
      let done = false;

      // ОДИН СТРОГИЙ ПОТОК ДЛЯ ЧТЕНИЯ: Никаких конфликтов
      async function getNextMatchedUrlsBatch(batchSize = 20) {
        let matchedBatch = [];
        
        while (!done && matchedBatch.length < batchSize) {
          const { value, done: streamDone } = await reader.read();
          if (streamDone) {
            done = true;
            if (currentLine.toLowerCase().includes(cleanQuery)) {
              matchedBatch.push(formatUrl(currentLine));
            }
            break;
          }

          currentLine += decoder.decode(value, { stream: true });
          const lines = currentLine.split("\n");
          currentLine = lines.pop() || "";

          for (const line of lines) {
            if (line.toLowerCase().includes(cleanQuery)) {
              matchedBatch.push(formatUrl(line));
              if (matchedBatch.length >= batchSize) break;
            }
          }
        }
        return matchedBatch;
      }

      function formatUrl(line) {
        const clean = line.split(",")[0].trim();
        return clean.startsWith("http") ? clean : `https://${clean}`;
      }

      // Находим первую порцию совпадений по URL
      const candidateUrls = await getNextMatchedUrlsBatch(40);

      // ПАРАЛЛЕЛЬНАЯ ПРОВЕРКА DNS: Запускаем до 4 проверок одновременно без конфликтов потока
      const checkDomainDns = async (targetUrl) => {
        try {
          const parsedUrl = new URL(targetUrl);
          const hostname = parsedUrl.hostname;

          const ipAddresses = await dns.resolve(hostname).catch(() => []);
          if (ipAddresses.length > 0) {
            return {
              url: targetUrl,
              domain: hostname,
              ip: ipAddresses[0] || "Unknown",
              title: hostname
            };
          }
        } catch (e) {
          // Игнорируем ошибки парсинга/недоступности доменов
        }
        return null;
      };

      // Пул запущенных параллельных задач
      const concurrencyLimit = 4;
      for (let i = 0; i < candidateUrls.length; i += concurrencyLimit) {
        if (finalResults.length >= 20) break;
        
        const chunk = candidateUrls.slice(i, i + concurrencyLimit);
        const chunkPromises = chunk.map(url => checkDomainDns(url));
        const chunkResults = await Promise.all(chunkPromises);

        for (const res of chunkResults) {
          if (res && finalResults.length < 20) {
            finalResults.push(res);
          }
        }
      }

      // Закрываем чтение, если вышли досрочно
      if (!done) await reader.cancel();

      return new Response(JSON.stringify(finalResults), { headers: corsHeaders });

    } catch (err) {
      return new Response(JSON.stringify({ 
        error: "ОШИБКА ВНУТРЕННЕЙ ЛОГИКИ WORKER", 
        details: err.message 
      }), { headers: corsHeaders, status: 500 });
    }
  }
};
