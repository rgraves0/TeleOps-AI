from __future__ import annotations

import logging
from urllib.parse import unquote
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class WebSearchPlugin:
    def __init__(self) -> None:
        self.base_url = "https://html.duckduckgo.com/html/"
        self.timeout = 30
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://duckduckgo.com/",
            "Connection": "keep-alive",
        }

    async def search(self, query: str, max_results: int = 5) -> str:
        if not query.strip():
            return "Search query is empty."

        params = {"q": query}
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code >= 400:
                logger.error(f"DuckDuckGo HTTP Search failed with status: {response.status_code}")
                return "Failed to retrieve search results due to server error."

            soup = BeautifulSoup(response.text, "html.parser")
            results = soup.find_all("div", class_="result")
            
            if not results:
                return "No search results found."

            search_lines = []
            for idx, res in enumerate(results[:max_results], 1):
                title_tag = res.find("a", class_="result__url")
                snippet_tag = res.find("a", class_="result__snippet")
                
                if not title_tag:
                    continue

                title = title_tag.get_text(strip=True)
                raw_url = title_tag.get("href", "")
                
                # DuckDuckGo outbound link redirection clean up
                url = raw_url
                if "/l/?kh=" in raw_url and "uddg=" in raw_url:
                    try:
                        url = unquote(raw_url.split("uddg=")[1].split("&")[0])
                    except Exception:
                        pass
                
                snippet = snippet_tag.get_text(strip=True) if snippet_tag else "No description available."
                search_lines.append(f"{idx}. Title: {title}\n   URL: {url}\n   Snippet: {snippet}\n")

            return "\n".join(search_lines)

        except Exception as exc:
            logger.exception(f"WebSearchPlugin search workflow failed: {exc}")
            return f"An error occurred during web search processing."

plugin = WebSearchPlugin()
