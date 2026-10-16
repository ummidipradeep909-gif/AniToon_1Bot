import os
import unittest
from unittest.mock import patch

import intelligence
from intelligence import (
    IntelligenceError,
    _clean_query,
    news_rss_intelligence,
    _safe_https_url,
    gemini_intelligence,
    intelligence_html,
    youtube_videos,
    videos_html,
)


class QueryAndUrlTests(unittest.TestCase):
    def test_query_is_trimmed_and_bounded(self):
        self.assertEqual(_clean_query("  current   web AI "), "current web AI")
        self.assertEqual(len(_clean_query("x" * 300)), 140)

    def test_empty_query_is_rejected(self):
        with self.assertRaises(IntelligenceError):
            _clean_query("   ")

    def test_only_https_links_are_accepted(self):
        self.assertEqual(_safe_https_url("https://example.com/a"), "https://example.com/a")
        self.assertIsNone(_safe_https_url("javascript:alert(1)"))
        self.assertIsNone(_safe_https_url("http://example.com"))
        self.assertIsNone(_safe_https_url("https://user:pass@example.com"))


class GeminiTests(unittest.TestCase):
    @patch.dict(os.environ, {"GEMINI_API_KEY": ""}, clear=False)
    def test_missing_key_is_a_clear_setup_error(self):
        with self.assertRaisesRegex(IntelligenceError, "GEMINI_API_KEY"):
            gemini_intelligence("media intelligence")

    @patch.dict(os.environ, {"GEMINI_API_KEY": "test-key", "GEMINI_MODEL": "gemini-3.5-flash-lite"}, clear=False)
    @patch("intelligence._json_request")
    def test_grounded_report_keeps_real_source_links(self, fake_request):
        intelligence._INTEL_CACHE.clear()
        fake_request.return_value = {
            "candidates": [{
                "content": {"parts": [{"text": "Recent developments are mixed.\nVerify the following claims."}]},
                "groundingMetadata": {
                    "webSearchQueries": ["media intelligence"],
                    "groundingChunks": [{
                        "web": {"uri": "https://example.com/report", "title": "Example report"}
                    }],
                },
            }]
        }
        result = gemini_intelligence("media intelligence")
        self.assertIn("Recent developments", result["text"])
        self.assertEqual(result["sources"][0]["url"], "https://example.com/report")
        rendered = intelligence_html(result)
        self.assertIn('href="https://example.com/report"', rendered)
        self.assertIn("Grounded sources", rendered)
        fake_request.assert_called_once()
        endpoint = fake_request.call_args.args[0]
        self.assertTrue(endpoint.endswith(":generateContent"))
        self.assertNotIn("test-key", endpoint)
        self.assertEqual(fake_request.call_args.kwargs["api_key"], "test-key")
        self.assertEqual(fake_request.call_args.kwargs["payload"]["tools"], [{"google_search": {}}])


class YouTubeTests(unittest.TestCase):
    def setUp(self):
        intelligence._YOUTUBE_CACHE.clear()

    @patch.dict(os.environ, {"YOUTUBE_API_KEY": ""}, clear=False)
    def test_no_api_key_returns_live_topic_search(self):
        result = youtube_videos("web development", shorts=True)
        self.assertFalse(result["configured"])
        self.assertIn("search_query=web+development+Shorts", result["fallback_url"])
        self.assertIn("YouTube Data API is not configured", videos_html(result))

    @patch.dict(os.environ, {"YOUTUBE_API_KEY": "test-key", "YOUTUBE_REGION_CODE": "IN"}, clear=False)
    @patch("intelligence._json_request")
    def test_api_video_result_contains_direct_youtube_link(self, fake_request):
        fake_request.return_value = {
            "items": [{
                "id": {"kind": "youtube#video", "videoId": "abcdefghijk"},
                "snippet": {
                    "title": "Web basics",
                    "channelTitle": "Example channel",
                    "publishedAt": "2026-01-01T10:00:00Z",
                    "description": "Intro to the web",
                },
            }]
        }
        result = youtube_videos("web development")
        self.assertEqual(result["videos"][0]["url"], "https://www.youtube.com/watch?v=abcdefghijk")
        output = videos_html(result)
        self.assertIn("Web basics", output)
        self.assertIn("Example channel", output)
        fake_request.assert_called_once()


class RssFallbackTests(unittest.TestCase):
    def setUp(self):
        intelligence._INTEL_CACHE.clear()

    @patch("intelligence.urlopen")
    def test_public_rss_produces_real_source_links_without_gemini_key(self, fake_urlopen):
        rss = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel><item>
          <title>New web platform update</title>
          <link>https://example.com/news</link>
          <pubDate>Fri, 16 Oct 2026 10:00:00 GMT</pubDate>
          <source>Example News</source>
          <description>Details about &lt;b&gt;web development&lt;/b&gt; update.</description>
        </item></channel></rss>"""
        fake_response = unittest.mock.MagicMock()
        fake_response.__enter__.return_value.read.return_value = rss
        fake_urlopen.return_value = fake_response
        with patch.dict(os.environ, {"GEMINI_API_KEY": ""}, clear=False):
            result = news_rss_intelligence("web development")
        self.assertEqual(result["mode"], "keyless_rss")
        self.assertIn("no Gemini analysis", result["text"])
        self.assertEqual(result["sources"][0]["url"], "https://example.com/news")
        self.assertEqual(result["sources"][0]["title"], "New web platform update")
        rendered = intelligence_html(result)
        self.assertIn("Keyless Media News Digest", rendered)
        self.assertIn('href="https://example.com/news"', rendered)
        fake_urlopen.assert_called_once()



if __name__ == "__main__":
    unittest.main()
