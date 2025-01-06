import unittest
from transformer.utils import flatten_event

data = {
  "app_id": "paymentcheck-uk",
  "platform": "web",
  "etl_tstamp": "2025-01-06T16:58:11.372Z",
  "collector_tstamp": "2025-01-06T16:58:10.859Z",
  "dvce_created_tstamp": "2025-01-06T16:58:10.346Z",
  "event": "page_ping",
  "event_id": "a8a40a4c-7a69-411b-a88f-73059a1818cc",
  "name_tracker": "hai",
  "v_tracker": "js-3.15.0",
  "v_collector": "ssc-2.10.3-kafka",
  "v_etl": "pragmatik-enrich-kafka-3.9.11",
  "user_ipaddress": "212.105.145.155",
  "domain_userid": "4b5889c0-23fa-4d21-84c8-19133254462a",
  "domain_sessionidx": 1,
  "network_userid": "9403a3b8-9214-4457-a3e4-e2d49afe37b4",
  "page_url": "https://546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com/",
  "page_title": "Company Payment Data & Financial Information | PaymentCheck",
  "page_referrer": "https://546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com/?forceHideBadge=true",
  "page_urlscheme": "https",
  "page_urlhost": "546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com",
  "page_urlport": 443,
  "page_urlpath": "/",
  "refr_urlscheme": "https",
  "refr_urlhost": "546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com",
  "refr_urlport": 443,
  "refr_urlpath": "/",
  "refr_urlquery": "forceHideBadge=true",
  "refr_medium": "internal",
  "contexts_com_snowplowanalytics_snowplow_web_page_1": [
    {
      "id": "f5e5af7d-2a63-4e56-ba8e-106412e0ec2f"
    }
  ],
  "contexts_com_snowplowanalytics_snowplow_browser_context_1": [
    {
      "viewport": "1778x1187",
      "documentSize": "1763x3190",
      "resolution": "2560x1440",
      "colorDepth": 24,
      "devicePixelRatio": 2,
      "cookiesEnabled": True,
      "online": True,
      "browserLanguage": "en-GB",
      "documentLanguage": "en",
      "webdriver": False,
      "deviceMemory": 8,
      "hardwareConcurrency": 8,
      "tabId": "b94c7023-1f4a-420e-8e7c-a12f135ab3ad"
    }
  ],
  "contexts_org_w3_performance_timing_1": [
    {
      "navigationStart": 1736174725027,
      "redirectStart": 0,
      "redirectEnd": 0,
      "fetchStart": 1736174725029,
      "domainLookupStart": 1736174725032,
      "domainLookupEnd": 1736174725068,
      "connectStart": 1736174725068,
      "secureConnectionStart": 1736174725105,
      "connectEnd": 1736174725304,
      "requestStart": 1736174725304,
      "responseStart": 1736174725403,
      "responseEnd": 1736174725407,
      "unloadEventStart": 0,
      "unloadEventEnd": 0,
      "domLoading": 1736174725407,
      "domInteractive": 1736174725432,
      "domContentLoadedEventStart": 1736174726301,
      "domContentLoadedEventEnd": 1736174726301,
      "domComplete": 1736174726303,
      "loadEventStart": 1736174726303,
      "loadEventEnd": 1736174726303
    }
  ],
  "contexts_org_ietf_http_client_hints_1": [
    {
      "isMobile": False,
      "brands": [
        {
          "brand": "Google Chrome",
          "version": "131"
        },
        {
          "brand": "Chromium",
          "version": "131"
        },
        {
          "brand": "Not_A Brand",
          "version": "24"
        }
      ],
      "architecture": "arm",
      "model": "",
      "platform": "macOS",
      "uaFullVersion": "131.0.6778.205",
      "platformVersion": "14.6.0"
    }
  ],
  "contexts_com_google_analytics_cookies_1": [
    {}
  ],
  "contexts_ai_pragmatik_incognito_1": [
    {
      "incognito_enabled": False,
      "browser_name": "Chrome"
    }
  ],
  "contexts_com_snowplowanalytics_snowplow_client_session_1": [
    {
      "userId": "4b5889c0-23fa-4d21-84c8-19133254462a",
      "sessionId": "56a48d88-8640-4771-81ca-477b03018d14",
      "eventIndex": 1,
      "sessionIndex": 1,
      "previousSessionId": None,
      "storageMechanism": "COOKIE_1",
      "firstEventId": "a8a40a4c-7a69-411b-a88f-73059a1818cc",
      "firstEventTimestamp": "2025-01-06T16:58:10.346Z"
    }
  ],
  "pp_yoffset_min": 80,
  "pp_yoffset_max": 80,
  "useragent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "br_lang": "en-GB",
  "br_cookies": True,
  "br_colordepth": "24",
  "br_viewwidth": 1778,
  "br_viewheight": 1187,
  "os_timezone": "Europe/London",
  "dvce_screenwidth": 2560,
  "dvce_screenheight": 1440,
  "doc_charset": "UTF-8",
  "doc_width": 1763,
  "doc_height": 3190,
  "dvce_sent_tstamp": "2025-01-06T16:58:10.347Z",
  "contexts_ai_pragmatik_ip-api_1": [
    {
      "as": "AS14593 Space Exploration Technologies Corporation",
      "asname": "SPACEX-STARLINK",
      "city": "London",
      "country": "United Kingdom",
      "countryCode": "GB",
      "district": "",
      "hosting": False,
      "isp": "Space Exploration Technologies Corporation",
      "lat": 51.5073,
      "lon": -0.1277,
      "mobile": False,
      "org": "Starlink Lndngbr1",
      "proxy": False,
      "region": "ENG",
      "regionName": "England",
      "reverse": "",
      "timezone": "Europe/London",
      "zip": ""
    }
  ],
  "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": [
    {
      "useragentFamily": "Chrome",
      "useragentMajor": "131",
      "useragentMinor": "0",
      "useragentPatch": "0",
      "useragentVersion": "Chrome 131.0.0",
      "osFamily": "Mac OS X",
      "osMajor": "10",
      "osMinor": "15",
      "osPatch": "7",
      "osPatchMinor": None,
      "osVersion": "Mac OS X 10.15.7",
      "deviceFamily": "Mac"
    }
  ],
  "contexts_com_iab_snowplow_spiders_and_robots_1": [
    {
      "spiderOrRobot": False,
      "category": "BROWSER",
      "reason": "PASSED_ALL",
      "primaryImpact": "NONE"
    }
  ],
  "domain_sessionid": "56a48d88-8640-4771-81ca-477b03018d14",
  "derived_tstamp": "2025-01-06T16:58:10.858Z",
  "event_vendor": "com.snowplowanalytics.snowplow",
  "event_name": "page_ping",
  "event_format": "jsonschema",
  "event_version": "1-0-0",
  "event_fingerprint": "2621cf6a93419c13f6b20fac03527708"
}

expected_output = {
  "app_id": "paymentcheck-uk",
  "platform": "web",
  "etl_tstamp": "2025-01-06T16:58:11.372Z",
  "collector_tstamp": "2025-01-06T16:58:10.859Z",
  "dvce_created_tstamp": "2025-01-06T16:58:10.346Z",
  "event": "page_ping",
  "event_id": "a8a40a4c-7a69-411b-a88f-73059a1818cc",
  "name_tracker": "hai",
  "v_tracker": "js-3.15.0",
  "v_collector": "ssc-2.10.3-kafka",
  "v_etl": "pragmatik-enrich-kafka-3.9.11",
  "user_ipaddress": "212.105.145.155",
  "domain_userid": "4b5889c0-23fa-4d21-84c8-19133254462a",
  "domain_sessionidx": 1,
  "network_userid": "9403a3b8-9214-4457-a3e4-e2d49afe37b4",
  "page_url": "https://546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com/",
  "page_title": "Company Payment Data & Financial Information | PaymentCheck",
  "page_referrer": "https://546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com/?forceHideBadge=true",
  "page_urlscheme": "https",
  "page_urlhost": "546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com",
  "page_urlport": 443,
  "page_urlpath": "/",
  "refr_urlscheme": "https",
  "refr_urlhost": "546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com",
  "refr_urlport": 443,
  "refr_urlpath": "/",
  "refr_urlquery": "forceHideBadge=true",
  "refr_medium": "internal",
  "web_page_id": "f5e5af7d-2a63-4e56-ba8e-106412e0ec2f",
  "browser_context_viewport": "1778x1187",
  "browser_context_document_size": "1763x3190",
  "browser_context_resolution": "2560x1440",
  "browser_context_color_depth": 24,
  "browser_context_device_pixel_ratio": 2,
  "browser_context_cookies_enabled": True,
  "browser_context_online": True,
  "browser_context_browser_language": "en-GB",
  "browser_context_document_language": "en",
  "browser_context_webdriver":   False,
  "browser_context_device_memory": 8,
  "browser_context_hardware_concurrency": 8,
  "browser_context_tab_id": "b94c7023-1f4a-420e-8e7c-a12f135ab3ad",
  "performance_timing_navigation_start": 1736174725027,
  "performance_timing_redirect_start": 0,
  "performance_timing_redirect_end": 0,
  "performance_timing_fetch_start": 1736174725029,
  "performance_timing_domain_lookup_start": 1736174725032,
  "performance_timing_domain_lookup_end": 1736174725068,
  "performance_timing_connect_start": 1736174725068,
  "performance_timing_secure_connection_start": 1736174725105,
  "performance_timing_connect_end": 1736174725304,
  "performance_timing_request_start": 1736174725304,
  "performance_timing_response_start": 1736174725403,
  "performance_timing_response_end": 1736174725407,
  "performance_timing_unload_event_start": 0,
  "performance_timing_unload_event_end": 0,
  "performance_timing_dom_loading": 1736174725407,
  "performance_timing_dom_interactive": 1736174725432,
  "performance_timing_dom_content_loaded_event_start": 1736174726301,
  "performance_timing_dom_content_loaded_event_end": 1736174726301,
  "performance_timing_dom_complete": 1736174726303,
  "performance_timing_load_event_start": 1736174726303,
  "performance_timing_load_event_end": 1736174726303,
  "http_client_hints_is_mobile":   False,
  "http_client_hints_brands": [
    {
      "brand": "Google Chrome",
      "version": "131"
    },
    {
      "brand": "Chromium",
      "version": "131"
    },
    {
      "brand": "Not_A Brand",
      "version": "24"
    }
  ],
  "http_client_hints_architecture": "arm",
  "http_client_hints_model": "",
  "http_client_hints_platform": "macOS",
  "http_client_hints_ua_full_version": "131.0.6778.205",
  "http_client_hints_platform_version": "14.6.0",
  "incognito_incognito_enabled":   False,
  "incognito_browser_name": "Chrome",
  "client_session_user_id": "4b5889c0-23fa-4d21-84c8-19133254462a",
  "client_session_session_id": "56a48d88-8640-4771-81ca-477b03018d14",
  "client_session_event_index": 1,
  "client_session_session_index": 1,
  "client_session_previous_session_id": None,
  "client_session_storage_mechanism": "COOKIE_1",
  "client_session_first_event_id": "a8a40a4c-7a69-411b-a88f-73059a1818cc",
  "client_session_first_event_timestamp": "2025-01-06T16:58:10.346Z",
  "pp_yoffset_min": 80,
  "pp_yoffset_max": 80,
  "useragent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "br_lang": "en-GB",
  "br_cookies": True,
  "br_colordepth": "24",
  "br_viewwidth": 1778,
  "br_viewheight": 1187,
  "os_timezone": "Europe/London",
  "dvce_screenwidth": 2560,
  "dvce_screenheight": 1440,
  "doc_charset": "UTF-8",
  "doc_width": 1763,
  "doc_height": 3190,
  "dvce_sent_tstamp": "2025-01-06T16:58:10.347Z",
  "ip_api_as": "AS14593 Space Exploration Technologies Corporation",
  "ip_api_asname": "SPACEX-STARLINK",
  "ip_api_city": "London",
  "ip_api_country": "United Kingdom",
  "ip_api_country_code": "GB",
  "ip_api_district": "",
  "ip_api_hosting":   False,
  "ip_api_isp": "Space Exploration Technologies Corporation",
  "ip_api_lat": 51.5073,
  "ip_api_lon": -0.1277,
  "ip_api_mobile":   False,
  "ip_api_org": "Starlink Lndngbr1",
  "ip_api_proxy":   False,
  "ip_api_region": "ENG",
  "ip_api_region_name": "England",
  "ip_api_reverse": "",
  "ip_api_timezone": "Europe/London",
  "ip_api_zip": "",
  "ua_useragent_family": "Chrome",
  "ua_useragent_major": "131",
  "ua_useragent_minor": "0",
  "ua_useragent_patch": "0",
  "ua_useragent_version": "Chrome 131.0.0",
  "ua_os_family": "Mac OS X",
  "ua_os_major": "10",
  "ua_os_minor": "15",
  "ua_os_patch": "7",
  "ua_os_patch_minor": None,
  "ua_os_version": "Mac OS X 10.15.7",
  "ua_device_family": "Mac",
  "spiders_and_robots_spider_or_robot":   False,
  "spiders_and_robots_category": "BROWSER",
  "spiders_and_robots_reason": "PASSED_ALL",
  "spiders_and_robots_primary_impact": "NONE",
  "domain_sessionid": "56a48d88-8640-4771-81ca-477b03018d14",
  "derived_tstamp": "2025-01-06T16:58:10.858Z",
  "event_vendor": "com.snowplowanalytics.snowplow",
  "event_name": "page_ping",
  "event_format": "jsonschema",
  "event_version": "1-0-0",
  "event_fingerprint": "2621cf6a93419c13f6b20fac03527708"
}

class TestUtils(unittest.TestCase):
  def test_flatten_event(self):
    self.assertEqual(flatten_event(data), expected_output)

if __name__ == "__main__":
  unittest.main()
