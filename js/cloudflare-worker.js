export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const targetFunc = url.searchParams.get("target_func");
    
    // === 核心配置 ===
    // 1. 板块列表接口
    const API_LIST = "http://17.push2.eastmoney.com/api/qt/clist/get";
    // 2. K线行情接口
    const API_KLINE = "http://push2his.eastmoney.com/api/qt/stock/kline/get";
    // 3. 【新增】资金流向历史接口
    const API_FLOW = "http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get";
    // 4. 【新增】成分股接口
    const API_CONST = "http://4.push2.eastmoney.com/api/qt/clist/get";
    
    let targetApi = "";
    if (targetFunc === "list") {
      targetApi = API_LIST;
    } else if (targetFunc === "kline") {
      targetApi = API_KLINE;
    } else if (targetFunc === "flow") {
      // 新增 flow 路由
      targetApi = API_FLOW;
    } else if (targetFunc === "constituents") {
      // 新增成分股路由
      targetApi = API_CONST;
    } else {
      return new Response("Error: Invalid 'target_func'. Use 'list', 'kline', 'flow', or 'constituents'.", { status: 400 });
    }

    // 重新构建 URL
    let newUrl = new URL(targetApi);
    url.searchParams.forEach((value, key) => {
      if (key !== "target_func") {
        newUrl.searchParams.append(key, value);
      }
    });

    const newRequest = new Request(newUrl, {
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "http://quote.eastmoney.com/",
        "Connection": "keep-alive"
      }
    });

    try {
      const response = await fetch(newRequest);
      return response;
    } catch (e) {
      return new Response(`Proxy Error: ${e.message}`, { status: 500 });
    }
  },
};
