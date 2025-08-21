/* eslint-disable @typescript-eslint/no-explicit-any,no-console */
import { getCacheTime, getConfig } from '@/lib/config';
import { searchFromApiStream } from '@/lib/downstream';
import { yellowWords } from '@/lib/yellow';

export const runtime = 'edge';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const query = searchParams.get('q');
  const streamParam = searchParams.get('stream');
  const enableStream = streamParam !== '0'; // 默认开启流式

  if (!query) {
    // 空查询，明确不缓存
    return new Response(JSON.stringify({ results: [] }), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store, no-cache, must-revalidate',
        Pragma: 'no-cache',
        Expires: '0',
      },
    });
  }

  const config = await getConfig();
  const apiSites = config.SourceConfig.filter((site) => !site.disabled);

  if (!enableStream) {
    // 非流式：聚合完成后根据是否为空设置缓存策略
    const aggregatedResults: any[] = [];
    for (const site of apiSites) {
      try {
        const generator = searchFromApiStream(site, query);
        for await (const pageResults of generator) {
          let filteredResults = pageResults;
          if (!config.SiteConfig.DisableYellowFilter) {
            filteredResults = pageResults.filter((result) => {
              const typeName = result.type_name || '';
              return !yellowWords.some((word) => typeName.includes(word));
            });
          }
          aggregatedResults.push(...filteredResults);
        }
      } catch (err: any) {
        console.warn(`搜索失败 ${site.name}:`, err.message);
      }
    }

    if (aggregatedResults.length === 0) {
      return new Response(JSON.stringify({ aggregatedResults }), {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          'Cache-Control': 'no-store, no-cache, must-revalidate',
          Pragma: 'no-cache',
          Expires: '0',
        },
      });
    } else {
      const cacheTime = await getCacheTime();
      return new Response(JSON.stringify({ aggregatedResults }), {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          'Cache-Control': `private, max-age=${cacheTime}`,
        },
      });
    }
  }

  // 流式：保持原有流式行为（无法在响应开始后再按“是否为空”调整缓存头）
  const encoder = new TextEncoder();
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();

  // 安全写入与断连处理
  let shouldStop = false;
  const abortSignal = (request as any).signal as AbortSignal | undefined;
  abortSignal?.addEventListener('abort', () => {
    shouldStop = true;
    try {
      writer.close();
    } catch {
      // ignore
    }
  });

  const safeWrite = async (obj: unknown) => {
    if (shouldStop || abortSignal?.aborted) return false;
    try {
      await writer.write(encoder.encode(JSON.stringify(obj) + '\n'));
      return true;
    } catch {
      shouldStop = true;
      return false;
    }
  };

  (async () => {
    const aggregatedResults: any[] = [];
    for (const site of apiSites) {
      try {
        const generator = searchFromApiStream(site, query);
        for await (const pageResults of generator) {
          let filteredResults = pageResults;
          if (!config.SiteConfig.DisableYellowFilter) {
            filteredResults = pageResults.filter((result) => {
              const typeName = result.type_name || '';
              return !yellowWords.some((word) => typeName.includes(word));
            });
          }
          aggregatedResults.push(...filteredResults);
          if (!(await safeWrite({ pageResults: filteredResults }))) {
            break;
          }
        }
      } catch (err: any) {
        console.warn(`搜索失败 ${site.name}:`, err.message);
      }
      if (shouldStop) break;
    }
    await safeWrite({ aggregatedResults });
    try {
      await writer.close();
    } catch {
      // ignore
    }
  })();

  const cacheTime = await getCacheTime();
  return new Response(readable, {
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': `private, max-age=${cacheTime}`,
    },
  });
}
