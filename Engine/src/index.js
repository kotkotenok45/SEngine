import dns from 'node:dns/promises';

const DOMAINS_TXT_URL = "https://raw.githubusercontent.com/zer0h/top-1000000-domains/master/top-10000-domains";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const query = url.searchParams.get("q") || "";
    // Приводим запрос к нижнему регистру и убираем пробелы
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

    try {
      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      
      let finalResults = [];
      let currentLine = "";
      let done = false;

      // Безопасное чтение потока с приведением к регистру
      async function getNextMatchedUrlsBatch(batchSize = 40) {
        let matchedBatch = [];
        
        while (!done && matchedBatch.length < batchSize) {
          const { value, done: streamDone } = await reader.read();
          if (streamDone) {
            done = true;
            // Проверяем последний хвостик файла
            const finalClean = currentLine.replace(/\r/g, "").trim();
            if (finalClean.toLowerCase().includes(cleanQuery) && finalClean.length > 0) {
              matchedBatch.push(formatUrl(finalClean));
            }
            break;
          }

          currentLine += decoder.decode(value, { stream: true });
          const lines = currentLine.split("\n");
          currentLine = lines.pop() || "";

          for (const line of lines) {
            // Очищаем от невидимых символов \r (Windows переносы строк)
            const cleanLine = line.replace(/\r/g, "").trim();
            if (cleanLine.toLowerCase().includes(cleanQuery) && cleanLine.length > 0) {
              matchedBatch.push(formatUrl(cleanLine));
              if (matchedBatch.length >= batchSize) break;
            }
          }
        }
        return matchedBatch;
      }

      // Жесткая очистка URL от мусора и CSV-запятых
      function formatUrl(line) {
        const parts = line.split(",");
        const clean = parts[parts.length - 1].trim() || parts[0].trim();
        return clean.startsWith("http") ? clean : `https://${clean}`;
      }

      // Получаем порцию доменов
      const candidateUrls = await getNextMatchedUrlsBatch(40);

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
          // Игнорируем ошибки конкретных сайтов
        }
        return null;
      };

      // Пробиваем их через DNS в 4 параллельных потока
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
