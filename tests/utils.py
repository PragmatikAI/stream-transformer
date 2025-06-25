import unittest
from transformer.utils import flatten_event, split_event

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
  "page_referrer": "https://546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com/?forceHideBadge=True",
  "page_urlscheme": "https",
  "page_urlhost": "546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com",
  "page_urlport": 443,
  "page_urlpath": "/",
  "refr_urlscheme": "https",
  "refr_urlhost": "546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com",
  "refr_urlport": 443,
  "refr_urlpath": "/",
  "refr_urlquery": "forceHideBadge=True",
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
  "page_referrer": "https://546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com/?forceHideBadge=True",
  "page_urlscheme": "https",
  "page_urlhost": "546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com",
  "page_urlport": 443,
  "page_urlpath": "/",
  "refr_urlscheme": "https",
  "refr_urlhost": "546463c7-4808-4efd-a3c4-59823a68afaf.lovableproject.com",
  "refr_urlport": 443,
  "refr_urlpath": "/",
  "refr_urlquery": "forceHideBadge=True",
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

unstruct_link_click_event = {"app_id": "7c743fef451c47ac9666d1a1", "platform": "web", "etl_tstamp": "2025-02-05T09:31:57.347Z", "collector_tstamp": "2025-02-05T09:31:53.544Z", "dvce_created_tstamp": "2025-02-05T09:31:53.372Z", "event": "unstruct", "event_id": "dbd359c3-7157-41a3-9883-b30e4376bd5d", "name_tracker": "hai", "v_tracker": "js-3.15.0", "v_collector": "ssc-2.9.0-kinesis", "v_etl": "snowplow-enrich-kinesis-3.8.0", "user_ipaddress": "90.28.186.170", "domain_userid": "aa76009c-a878-4931-828f-93ed033fb033", "domain_sessionidx": 3, "network_userid": "1e96c714-7a3a-41b9-bdae-f265e61c0e92", "page_url": "https://www.bullionbypost.eu/silver-price/silver-price-per-ounce/", "page_referrer": "https://www.bullionbypost.eu/gold-price/gold-price-per-ounce/", "page_urlscheme": "https", "page_urlhost": "www.bullionbypost.eu", "page_urlport": 443, "page_urlpath": "/silver-price/silver-price-per-ounce/", "refr_urlscheme": "https", "refr_urlhost": "www.bullionbypost.eu", "refr_urlport": 443, "refr_urlpath": "/gold-price/gold-price-per-ounce/", "refr_medium": "internal", "contexts_com_snowplowanalytics_snowplow_web_page_1": [{"id": "1cafa4e6-f2bb-465e-91fa-9c4888d435f4"}], "contexts_com_snowplowanalytics_snowplow_browser_context_1": [{"viewport": "1528x834", "documentSize": "1513x3170", "resolution": "1536x960", "colorDepth": 24, "devicePixelRatio": 1.25, "cookiesEnabled": True, "online": True, "browserLanguage": "en-GB", "documentLanguage": "en-gb", "webdriver": False, "deviceMemory": 8, "hardwareConcurrency": 4, "tabId": "b2340c26-d07f-4531-805e-9b6c7688ecb5"}], "contexts_org_w3_performance_timing_1": [{"navigationStart": 1738747876600, "redirectStart": 0, "redirectEnd": 0, "fetchStart": 1738747876618, "domainLookupStart": 1738747876618, "domainLookupEnd": 1738747876618, "connectStart": 1738747876618, "secureConnectionStart": 0, "connectEnd": 1738747876618, "requestStart": 1738747876622, "responseStart": 1738747876654, "responseEnd": 1738747876799, "unloadEventStart": 0, "unloadEventEnd": 0, "domLoading": 1738747876844, "domInteractive": 1738747876871, "domContentLoadedEventStart": 1738747876876, "domContentLoadedEventEnd": 1738747876876, "domComplete": 1738747876913, "loadEventStart": 1738747876913, "loadEventEnd": 1738747876914}], "contexts_org_ietf_http_client_hints_1": [{"isMobile": False, "brands": [{"brand": "Not A(Brand", "version": "8"}, {"brand": "Chromium", "version": "132"}, {"brand": "Microsoft Edge", "version": "132"}], "architecture": "x86", "model": "", "platform": "Windows", "uaFullVersion": "132.0.2957.127", "platformVersion": "19.0.0"}], "contexts_com_google_analytics_cookies_1": [{"_ga": "GA1.1.1877606939.1738663884"}], "contexts_co_hedingham_client-id_1": [{}], "contexts_uk_co_bullionbypost_hid_1": [{"hedingham_id": "1e96c714-7a3a-41b9-bdae-f265e61c0e92"}], "contexts_co_hedingham_incognito_1": [{"incognito_enabled": False, "browser_name": "Edge"}], "contexts_com_snowplowanalytics_snowplow_client_session_1": [{"userId": "aa76009c-a878-4931-828f-93ed033fb033", "sessionId": "9acf72da-9b86-44a7-9c5f-be291ce61703", "eventIndex": 17, "sessionIndex": 3, "previousSessionId": "2985b5b7-e1e7-4166-9b5c-2903e0dadb0d", "storageMechanism": "COOKIE_1", "firstEventId": "4f12ac44-d22d-4558-b285-092012a288dd", "firstEventTimestamp": "2025-02-05T09:30:09.722Z"}], "unstruct_event_com_snowplowanalytics_snowplow_link_click_1": {"targetUrl": "https://www.bullionbypost.eu/silver-price/silver-price-eur/", "elementId": "", "elementClasses": ["highcharts-label", "btn", "btn-outline-secondary"], "elementTarget": ""}, "useragent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0", "br_lang": "en-GB", "br_cookies": True, "br_colordepth": "24", "br_viewwidth": 1528, "br_viewheight": 834, "os_timezone": "Europe/Berlin", "dvce_screenwidth": 1536, "dvce_screenheight": 960, "doc_charset": "UTF-8", "doc_width": 1513, "doc_height": 3170, "dvce_sent_tstamp": "2025-02-05T09:31:53.374Z", "contexts_co_hedingham_ip-api_1": [{"as": "AS3215 Orange S.A.", "asname": "AS3215", "city": "Guingamp", "country": "France", "countryCode": "FR", "district": "", "hosting": False, "isp": "France Telecom", "lat": 48.5586, "lon": -3.1531, "mobile": False, "org": "", "proxy": False, "region": "BRE", "regionName": "Brittany", "reverse": "amontpellier-656-1-402-170.w90-28.abo.wanadoo.fr", "timezone": "Europe/Paris", "zip": "22200"}], "contexts_nl_basjes_yauaa_context_1": [{"deviceBrand": "Unknown", "deviceName": "Desktop", "operatingSystemVersionMajor": ">=10", "layoutEngineNameVersion": "Blink 132", "operatingSystemNameVersion": "Windows >=10", "agentInformationEmail": "Unknown", "networkType": "Unknown", "webviewAppNameVersionMajor": "Unknown ??", "layoutEngineNameVersionMajor": "Blink 132", "operatingSystemName": "Windows NT", "agentVersionMajor": "132", "layoutEngineVersionMajor": "132", "webviewAppName": "Unknown", "deviceClass": "Desktop", "agentNameVersionMajor": "Edge 132", "operatingSystemNameVersionMajor": "Windows >=10", "deviceCpuBits": "64", "webviewAppVersionMajor": "??", "operatingSystemClass": "Desktop", "webviewAppVersion": "??", "layoutEngineName": "Blink", "agentName": "Edge", "agentVersion": "132", "layoutEngineClass": "Browser", "agentNameVersion": "Edge 132", "operatingSystemVersion": ">=10", "deviceCpu": "Intel x86_64", "agentClass": "Browser", "layoutEngineVersion": "132", "agentInformationUrl": "Unknown"}], "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": [{"useragentFamily": "Edge", "useragentMajor": "132", "useragentMinor": "0", "useragentPatch": "0", "useragentVersion": "Edge 132.0.0", "osFamily": "Windows", "osMajor": "10", "osMinor": None, "osPatch": None, "osPatchMinor": None, "osVersion": "Windows 10", "deviceFamily": "Other"}], "contexts_com_iab_snowplow_spiders_and_robots_1": [{"spiderOrRobot": False, "category": "BROWSER", "reason": "PASSED_ALL", "primaryImpact": "NONE"}], "domain_sessionid": "9acf72da-9b86-44a7-9c5f-be291ce61703", "derived_tstamp": "2025-02-05T09:31:53.542Z", "event_vendor": "com.snowplowanalytics.snowplow", "event_name": "link_click", "event_format": "jsonschema", "event_version": "1-0-1", "event_fingerprint": "b60fc09adcf5d55bf33999f904a696fe", "True_tstamp": "\nTTZ"}

expected_unstruct_link_click_event = {"app_id": "7c743fef451c47ac9666d1a1", "platform": "web", "etl_tstamp": "2025-02-05T09:31:57.347Z", "collector_tstamp": "2025-02-05T09:31:53.544Z", "dvce_created_tstamp": "2025-02-05T09:31:53.372Z", "event": "unstruct", "event_id": "dbd359c3-7157-41a3-9883-b30e4376bd5d", "name_tracker": "hai", "v_tracker": "js-3.15.0", "v_collector": "ssc-2.9.0-kinesis", "v_etl": "snowplow-enrich-kinesis-3.8.0", "user_ipaddress": "90.28.186.170", "domain_userid": "aa76009c-a878-4931-828f-93ed033fb033", "domain_sessionidx": 3, "network_userid": "1e96c714-7a3a-41b9-bdae-f265e61c0e92", "page_url": "https://www.bullionbypost.eu/silver-price/silver-price-per-ounce/", "page_referrer": "https://www.bullionbypost.eu/gold-price/gold-price-per-ounce/", "page_urlscheme": "https", "page_urlhost": "www.bullionbypost.eu", "page_urlport": 443, "page_urlpath": "/silver-price/silver-price-per-ounce/", "refr_urlscheme": "https", "refr_urlhost": "www.bullionbypost.eu", "refr_urlport": 443, "refr_urlpath": "/gold-price/gold-price-per-ounce/", "refr_medium": "internal", "web_page_id": "1cafa4e6-f2bb-465e-91fa-9c4888d435f4", "browser_context_viewport": "1528x834", "browser_context_document_size": "1513x3170", "browser_context_resolution": "1536x960", "browser_context_color_depth": 24, "browser_context_device_pixel_ratio": 1.25, "browser_context_cookies_enabled": True, "browser_context_online": True, "browser_context_browser_language": "en-GB", "browser_context_document_language": "en-gb", "browser_context_webdriver": False, "browser_context_device_memory": 8, "browser_context_hardware_concurrency": 4, "browser_context_tab_id": "b2340c26-d07f-4531-805e-9b6c7688ecb5", "performance_timing_navigation_start": 1738747876600, "performance_timing_redirect_start": 0, "performance_timing_redirect_end": 0, "performance_timing_fetch_start": 1738747876618, "performance_timing_domain_lookup_start": 1738747876618, "performance_timing_domain_lookup_end": 1738747876618, "performance_timing_connect_start": 1738747876618, "performance_timing_secure_connection_start": 0, "performance_timing_connect_end": 1738747876618, "performance_timing_request_start": 1738747876622, "performance_timing_response_start": 1738747876654, "performance_timing_response_end": 1738747876799, "performance_timing_unload_event_start": 0, "performance_timing_unload_event_end": 0, "performance_timing_dom_loading": 1738747876844, "performance_timing_dom_interactive": 1738747876871, "performance_timing_dom_content_loaded_event_start": 1738747876876, "performance_timing_dom_content_loaded_event_end": 1738747876876, "performance_timing_dom_complete": 1738747876913, "performance_timing_load_event_start": 1738747876913, "performance_timing_load_event_end": 1738747876914, "http_client_hints_is_mobile": False, "http_client_hints_brands": [{"brand": "Not A(Brand", "version": "8"}, {"brand": "Chromium", "version": "132"}, {"brand": "Microsoft Edge", "version": "132"}], "http_client_hints_architecture": "x86", "http_client_hints_model": "", "http_client_hints_platform": "Windows", "http_client_hints_ua_full_version": "132.0.2957.127", "http_client_hints_platform_version": "19.0.0", "ga_cookies__ga": "GA1.1.1877606939.1738663884", "bbp_hid_hedingham_id": "1e96c714-7a3a-41b9-bdae-f265e61c0e92", "incognito_incognito_enabled": False, "incognito_browser_name": "Edge", "client_session_user_id": "aa76009c-a878-4931-828f-93ed033fb033", "client_session_session_id": "9acf72da-9b86-44a7-9c5f-be291ce61703", "client_session_event_index": 17, "client_session_session_index": 3, "client_session_previous_session_id": "2985b5b7-e1e7-4166-9b5c-2903e0dadb0d", "client_session_storage_mechanism": "COOKIE_1", "client_session_first_event_id": "4f12ac44-d22d-4558-b285-092012a288dd", "client_session_first_event_timestamp": "2025-02-05T09:30:09.722Z", "link_click_target_url": "https://www.bullionbypost.eu/silver-price/silver-price-eur/", "link_click_element_id": "", "link_click_element_classes": ["highcharts-label", "btn", "btn-outline-secondary"], "link_click_element_target": "", "useragent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0", "br_lang": "en-GB", "br_cookies": True, "br_colordepth": "24", "br_viewwidth": 1528, "br_viewheight": 834, "os_timezone": "Europe/Berlin", "dvce_screenwidth": 1536, "dvce_screenheight": 960, "doc_charset": "UTF-8", "doc_width": 1513, "doc_height": 3170, "dvce_sent_tstamp": "2025-02-05T09:31:53.374Z", "ip_api_as": "AS3215 Orange S.A.", "ip_api_asname": "AS3215", "ip_api_city": "Guingamp", "ip_api_country": "France", "ip_api_country_code": "FR", "ip_api_district": "", "ip_api_hosting": False, "ip_api_isp": "France Telecom", "ip_api_lat": 48.5586, "ip_api_lon": -3.1531, "ip_api_mobile": False, "ip_api_org": "", "ip_api_proxy": False, "ip_api_region": "BRE", "ip_api_region_name": "Brittany", "ip_api_reverse": "amontpellier-656-1-402-170.w90-28.abo.wanadoo.fr", "ip_api_timezone": "Europe/Paris", "ip_api_zip": "22200", "yauaa_device_brand": "Unknown", "yauaa_device_name": "Desktop", "yauaa_operating_system_version_major": ">=10", "yauaa_layout_engine_name_version": "Blink 132", "yauaa_operating_system_name_version": "Windows >=10", "yauaa_agent_information_email": "Unknown", "yauaa_network_type": "Unknown", "yauaa_webview_app_name_version_major": "Unknown ??", "yauaa_layout_engine_name_version_major": "Blink 132", "yauaa_operating_system_name": "Windows NT", "yauaa_agent_version_major": "132", "yauaa_layout_engine_version_major": "132", "yauaa_webview_app_name": "Unknown", "yauaa_device_class": "Desktop", "yauaa_agent_name_version_major": "Edge 132", "yauaa_operating_system_name_version_major": "Windows >=10", "yauaa_device_cpu_bits": "64", "yauaa_webview_app_version_major": "??", "yauaa_operating_system_class": "Desktop", "yauaa_webview_app_version": "??", "yauaa_layout_engine_name": "Blink", "yauaa_agent_name": "Edge", "yauaa_agent_version": "132", "yauaa_layout_engine_class": "Browser", "yauaa_agent_name_version": "Edge 132", "yauaa_operating_system_version": ">=10", "yauaa_device_cpu": "Intel x86_64", "yauaa_agent_class": "Browser", "yauaa_layout_engine_version": "132", "yauaa_agent_information_url": "Unknown", "ua_useragent_family": "Edge", "ua_useragent_major": "132", "ua_useragent_minor": "0", "ua_useragent_patch": "0", "ua_useragent_version": "Edge 132.0.0", "ua_os_family": "Windows", "ua_os_major": "10", "ua_os_minor": None, "ua_os_patch": None, "ua_os_patch_minor": None, "ua_os_version": "Windows 10", "ua_device_family": "Other", "spiders_and_robots_spider_or_robot": False, "spiders_and_robots_category": "BROWSER", "spiders_and_robots_reason": "PASSED_ALL", "spiders_and_robots_primary_impact": "NONE", "domain_sessionid": "9acf72da-9b86-44a7-9c5f-be291ce61703", "derived_tstamp": "2025-02-05T09:31:53.542Z", "event_vendor": "com.snowplowanalytics.snowplow", "event_name": "link_click", "event_format": "jsonschema", "event_version": "1-0-1", "event_fingerprint": "b60fc09adcf5d55bf33999f904a696fe", "True_tstamp": "\nTTZ"}

unstruct_ecomm_event = {"app_id": "62e12d5a1e81183127518f0b", "platform": "web", "etl_tstamp": "2025-02-05T09:31:57.154Z", "collector_tstamp": "2025-02-05T09:31:51.590Z", "dvce_created_tstamp": "2025-02-05T09:31:51.529Z", "event": "unstruct", "event_id": "e6c94fb8-71a9-4a70-8f20-585ba158d4e6", "name_tracker": "hai", "v_tracker": "js-3.15.0", "v_collector": "ssc-2.9.0-kinesis", "v_etl": "snowplow-enrich-kinesis-3.8.0", "user_ipaddress": "84.67.44.64", "domain_userid": "0f0ef97e-dfa4-4323-9787-0060e1fe7bfa", "domain_sessionidx": 153, "network_userid": "356bfd9c-6c4b-4ab3-b48a-7584b79c653e", "page_url": "https://www.bullionbypost.co.uk/gold-bars/1-ounce-gold-bar/1oz-gold-bar-value/", "page_referrer": "https://www.bullionbypost.co.uk/gold-bars/1-ounce-gold-bar/1-ounce-fine-gold-bullion-bar/", "page_urlscheme": "https", "page_urlhost": "www.bullionbypost.co.uk", "page_urlport": 443, "page_urlpath": "/gold-bars/1-ounce-gold-bar/1oz-gold-bar-value/", "refr_urlscheme": "https", "refr_urlhost": "www.bullionbypost.co.uk", "refr_urlport": 443, "refr_urlpath": "/gold-bars/1-ounce-gold-bar/1-ounce-fine-gold-bullion-bar/", "refr_medium": "internal", "contexts_com_snowplowanalytics_snowplow_ecommerce_product_1": [{"currency": "GBP", "id": "450", "name": "1oz Gold Bars Best Value (Brand New)", "list_price": 84.2, "price": 84.2, "brand": "Mixed LBMA approved refiners", "category": "1oz Gold Bars", "quantity": None}], "contexts_com_snowplowanalytics_snowplow_web_page_1": [{"id": "083448e4-c20e-40c4-9587-a7439a0d74db"}], "contexts_com_snowplowanalytics_snowplow_browser_context_1": [{"viewport": "385x646", "documentSize": "385x5656", "resolution": "375x812", "colorDepth": 24, "devicePixelRatio": 3, "cookiesEnabled": True, "online": True, "browserLanguage": "en-GB", "documentLanguage": "en-gb", "webdriver": False, "hardwareConcurrency": 4, "tabId": "c23340f3-7393-44ed-b8cb-856e062ac378"}], "contexts_org_w3_performance_timing_1": [{"navigationStart": 1738747910799, "redirectStart": 0, "redirectEnd": 0, "fetchStart": 1738747910808, "domainLookupStart": 1738747910808, "domainLookupEnd": 1738747910808, "connectStart": 1738747910808, "secureConnectionStart": 0, "connectEnd": 1738747910808, "requestStart": 1738747910809, "responseStart": 1738747910836, "responseEnd": 1738747910836, "unloadEventStart": 0, "unloadEventEnd": 0, "domLoading": 1738747911374, "domInteractive": 1738747911381, "domContentLoadedEventStart": 1738747911383, "domContentLoadedEventEnd": 1738747911406, "domComplete": 1738747911406, "loadEventStart": 1738747911406, "loadEventEnd": 1738747911407}], "contexts_com_google_analytics_cookies_1": [{"_ga": "GA1.1.290782674.1730142045"}], "contexts_com_snowplowanalytics_snowplow_ecommerce_page_1": [{"type": "product"}], "contexts_co_hedingham_client-id_1": [{}], "contexts_uk_co_bullionbypost_hid_1": [{"hedingham_id": "356bfd9c-6c4b-4ab3-b48a-7584b79c653e"}], "contexts_co_hedingham_incognito_1": [{"incognito_enabled": False, "browser_name": "Safari"}], "contexts_com_snowplowanalytics_snowplow_client_session_1": [{"userId": "0f0ef97e-dfa4-4323-9787-0060e1fe7bfa", "sessionId": "fca9dc3b-f8fa-4e83-8d05-9e65eff7649f", "eventIndex": 101, "sessionIndex": 153, "previousSessionId": "2b8cc744-5d99-4a5a-bcd5-0e784396f2e8", "storageMechanism": "COOKIE_1", "firstEventId": "8518403f-7ddd-4de2-bb4b-839e92ff2db0", "firstEventTimestamp": "2025-02-05T08:52:21.336Z"}], "unstruct_event_com_snowplowanalytics_snowplow_ecommerce_snowplow_ecommerce_action_1": {"type": "product_view"}, "useragent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) GSA/354.0.720749604 Mobile/15E148 Safari/604.1", "br_lang": "en-GB", "br_cookies": True, "br_colordepth": "24", "br_viewwidth": 385, "br_viewheight": 646, "os_timezone": "Europe/London", "dvce_screenwidth": 375, "dvce_screenheight": 812, "doc_charset": "UTF-8", "doc_width": 385, "doc_height": 5656, "dvce_sent_tstamp": "2025-02-05T09:31:51.575Z", "contexts_co_hedingham_ip-api_1": [{"as": "AS5378 Vodafone Limited", "asname": "unspecified", "city": "Milton Keynes", "country": "United Kingdom", "countryCode": "GB", "district": "", "hosting": False, "isp": "Vodafone Limited", "lat": 52.0675, "lon": -0.7569, "mobile": False, "org": "Energis UK", "proxy": False, "region": "ENG", "regionName": "England", "reverse": "", "timezone": "Europe/London", "zip": "MK14"}], "contexts_nl_basjes_yauaa_context_1": [{"deviceBrand": "Apple", "deviceName": "Apple iPhone", "operatingSystemVersionMajor": "18", "layoutEngineNameVersion": "AppleWebKit 605.1.15", "deviceVersion": "iPhone", "operatingSystemNameVersion": "iOS 18.1.1", "agentInformationEmail": "Unknown", "networkType": "Unknown", "webviewAppNameVersionMajor": "Google Search App 354", "layoutEngineNameVersionMajor": "AppleWebKit 605", "operatingSystemName": "iOS", "agentVersionMajor": "605", "layoutEngineVersionMajor": "605", "webviewAppName": "Google Search App", "deviceClass": "Phone", "agentNameVersionMajor": "UIWebView 605", "operatingSystemNameVersionMajor": "iOS 18", "deviceFirmwareVersion": "15E148", "webviewAppVersionMajor": "354", "operatingSystemClass": "Mobile", "webviewAppVersion": "354.0.720749604", "layoutEngineName": "AppleWebKit", "agentName": "UIWebView", "agentVersion": "605.1.15", "layoutEngineClass": "Browser", "agentNameVersion": "UIWebView 605.1.15", "operatingSystemVersion": "18.1.1", "agentClass": "Browser Webview", "layoutEngineVersion": "605.1.15"}], "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": [{"useragentFamily": "Google", "useragentMajor": "354", "useragentMinor": "0", "useragentPatch": "720749604", "useragentVersion": "Google 354.0.720749604", "osFamily": "iOS", "osMajor": "18", "osMinor": "1", "osPatch": "1", "osPatchMinor": None, "osVersion": "iOS 18.1.1", "deviceFamily": "iPhone"}], "contexts_com_iab_snowplow_spiders_and_robots_1": [{"spiderOrRobot": False, "category": "BROWSER", "reason": "PASSED_ALL", "primaryImpact": "NONE"}], "domain_sessionid": "fca9dc3b-f8fa-4e83-8d05-9e65eff7649f", "derived_tstamp": "2025-02-05T09:31:51.544Z", "event_vendor": "com.snowplowanalytics.snowplow.ecommerce", "event_name": "snowplow_ecommerce_action", "event_format": "jsonschema", "event_version": "1-0-2", "event_fingerprint": "6ace870641ec50b081ca9b15e5c97353", "True_tstamp": "\nTTZ"}

expected_unstruct_ecomm_event = {"app_id": "62e12d5a1e81183127518f0b", "platform": "web", "etl_tstamp": "2025-02-05T09:31:57.154Z", "collector_tstamp": "2025-02-05T09:31:51.590Z", "dvce_created_tstamp": "2025-02-05T09:31:51.529Z", "event": "unstruct", "event_id": "e6c94fb8-71a9-4a70-8f20-585ba158d4e6", "name_tracker": "hai", "v_tracker": "js-3.15.0", "v_collector": "ssc-2.9.0-kinesis", "v_etl": "snowplow-enrich-kinesis-3.8.0", "user_ipaddress": "84.67.44.64", "domain_userid": "0f0ef97e-dfa4-4323-9787-0060e1fe7bfa", "domain_sessionidx": 153, "network_userid": "356bfd9c-6c4b-4ab3-b48a-7584b79c653e", "page_url": "https://www.bullionbypost.co.uk/gold-bars/1-ounce-gold-bar/1oz-gold-bar-value/", "page_referrer": "https://www.bullionbypost.co.uk/gold-bars/1-ounce-gold-bar/1-ounce-fine-gold-bullion-bar/", "page_urlscheme": "https", "page_urlhost": "www.bullionbypost.co.uk", "page_urlport": 443, "page_urlpath": "/gold-bars/1-ounce-gold-bar/1oz-gold-bar-value/", "refr_urlscheme": "https", "refr_urlhost": "www.bullionbypost.co.uk", "refr_urlport": 443, "refr_urlpath": "/gold-bars/1-ounce-gold-bar/1-ounce-fine-gold-bullion-bar/", "refr_medium": "internal", "ecommerce_product_currency": "GBP", "ecommerce_product_id": "450", "ecommerce_product_name": "1oz Gold Bars Best Value (Brand New)", "ecommerce_product_list_price": 84.2, "ecommerce_product_price": 84.2, "ecommerce_product_brand": "Mixed LBMA approved refiners", "ecommerce_product_category": "1oz Gold Bars", "ecommerce_product_quantity": None, "web_page_id": "083448e4-c20e-40c4-9587-a7439a0d74db", "browser_context_viewport": "385x646", "browser_context_document_size": "385x5656", "browser_context_resolution": "375x812", "browser_context_color_depth": 24, "browser_context_device_pixel_ratio": 3, "browser_context_cookies_enabled": True, "browser_context_online": True, "browser_context_browser_language": "en-GB", "browser_context_document_language": "en-gb", "browser_context_webdriver": False, "browser_context_hardware_concurrency": 4, "browser_context_tab_id": "c23340f3-7393-44ed-b8cb-856e062ac378", "performance_timing_navigation_start": 1738747910799, "performance_timing_redirect_start": 0, "performance_timing_redirect_end": 0, "performance_timing_fetch_start": 1738747910808, "performance_timing_domain_lookup_start": 1738747910808, "performance_timing_domain_lookup_end": 1738747910808, "performance_timing_connect_start": 1738747910808, "performance_timing_secure_connection_start": 0, "performance_timing_connect_end": 1738747910808, "performance_timing_request_start": 1738747910809, "performance_timing_response_start": 1738747910836, "performance_timing_response_end": 1738747910836, "performance_timing_unload_event_start": 0, "performance_timing_unload_event_end": 0, "performance_timing_dom_loading": 1738747911374, "performance_timing_dom_interactive": 1738747911381, "performance_timing_dom_content_loaded_event_start": 1738747911383, "performance_timing_dom_content_loaded_event_end": 1738747911406, "performance_timing_dom_complete": 1738747911406, "performance_timing_load_event_start": 1738747911406, "performance_timing_load_event_end": 1738747911407, "ga_cookies__ga": "GA1.1.290782674.1730142045", "ecommerce_page_type": "product", "bbp_hid_hedingham_id": "356bfd9c-6c4b-4ab3-b48a-7584b79c653e", "incognito_incognito_enabled": False, "incognito_browser_name": "Safari", "client_session_user_id": "0f0ef97e-dfa4-4323-9787-0060e1fe7bfa", "client_session_session_id": "fca9dc3b-f8fa-4e83-8d05-9e65eff7649f", "client_session_event_index": 101, "client_session_session_index": 153, "client_session_previous_session_id": "2b8cc744-5d99-4a5a-bcd5-0e784396f2e8", "client_session_storage_mechanism": "COOKIE_1", "client_session_first_event_id": "8518403f-7ddd-4de2-bb4b-839e92ff2db0", "client_session_first_event_timestamp": "2025-02-05T08:52:21.336Z", "ecommerce_action_type": "product_view", "useragent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) GSA/354.0.720749604 Mobile/15E148 Safari/604.1", "br_lang": "en-GB", "br_cookies": True, "br_colordepth": "24", "br_viewwidth": 385, "br_viewheight": 646, "os_timezone": "Europe/London", "dvce_screenwidth": 375, "dvce_screenheight": 812, "doc_charset": "UTF-8", "doc_width": 385, "doc_height": 5656, "dvce_sent_tstamp": "2025-02-05T09:31:51.575Z", "ip_api_as": "AS5378 Vodafone Limited", "ip_api_asname": "unspecified", "ip_api_city": "Milton Keynes", "ip_api_country": "United Kingdom", "ip_api_country_code": "GB", "ip_api_district": "", "ip_api_hosting": False, "ip_api_isp": "Vodafone Limited", "ip_api_lat": 52.0675, "ip_api_lon": -0.7569, "ip_api_mobile": False, "ip_api_org": "Energis UK", "ip_api_proxy": False, "ip_api_region": "ENG", "ip_api_region_name": "England", "ip_api_reverse": "", "ip_api_timezone": "Europe/London", "ip_api_zip": "MK14", "yauaa_device_brand": "Apple", "yauaa_device_name": "Apple iPhone", "yauaa_operating_system_version_major": "18", "yauaa_layout_engine_name_version": "AppleWebKit 605.1.15", "yauaa_device_version": "iPhone", "yauaa_operating_system_name_version": "iOS 18.1.1", "yauaa_agent_information_email": "Unknown", "yauaa_network_type": "Unknown", "yauaa_webview_app_name_version_major": "Google Search App 354", "yauaa_layout_engine_name_version_major": "AppleWebKit 605", "yauaa_operating_system_name": "iOS", "yauaa_agent_version_major": "605", "yauaa_layout_engine_version_major": "605", "yauaa_webview_app_name": "Google Search App", "yauaa_device_class": "Phone", "yauaa_agent_name_version_major": "UIWebView 605", "yauaa_operating_system_name_version_major": "iOS 18", "yauaa_device_firmware_version": "15E148", "yauaa_webview_app_version_major": "354", "yauaa_operating_system_class": "Mobile", "yauaa_webview_app_version": "354.0.720749604", "yauaa_layout_engine_name": "AppleWebKit", "yauaa_agent_name": "UIWebView", "yauaa_agent_version": "605.1.15", "yauaa_layout_engine_class": "Browser", "yauaa_agent_name_version": "UIWebView 605.1.15", "yauaa_operating_system_version": "18.1.1", "yauaa_agent_class": "Browser Webview", "yauaa_layout_engine_version": "605.1.15", "ua_useragent_family": "Google", "ua_useragent_major": "354", "ua_useragent_minor": "0", "ua_useragent_patch": "720749604", "ua_useragent_version": "Google 354.0.720749604", "ua_os_family": "iOS", "ua_os_major": "18", "ua_os_minor": "1", "ua_os_patch": "1", "ua_os_patch_minor": None, "ua_os_version": "iOS 18.1.1", "ua_device_family": "iPhone", "spiders_and_robots_spider_or_robot": False, "spiders_and_robots_category": "BROWSER", "spiders_and_robots_reason": "PASSED_ALL", "spiders_and_robots_primary_impact": "NONE", "domain_sessionid": "fca9dc3b-f8fa-4e83-8d05-9e65eff7649f", "derived_tstamp": "2025-02-05T09:31:51.544Z", "event_vendor": "com.snowplowanalytics.snowplow.ecommerce", "event_name": "snowplow_ecommerce_action", "event_format": "jsonschema", "event_version": "1-0-2", "event_fingerprint": "6ace870641ec50b081ca9b15e5c97353", "True_tstamp": "\nTTZ"}

list_view_event={"app_id": "\n  5bfef223baff46639e33dfef", "platform": "web", "etl_tstamp": "2024-12-14T11:59:40.152Z", "collector_tstamp": "2024-12-14T11:59:38.673Z", "dvce_created_tstamp": "2024-12-14T11:59:37.901Z", "event": "unstruct", "event_id": "e46fd90c-dbf5-40c4-950c-b4882f6cd153", "name_tracker": "hai", "v_tracker": "js-3.15.0", "v_collector": "ssc-2.9.0-kinesis", "v_etl": "snowplow-enrich-kinesis-3.8.0", "user_ipaddress": "2.123.215.228", "domain_userid": "9fd7b1e8-a7b0-4b98-b8ba-c947878a0ad8", "domain_sessionidx": 1, "network_userid": "dbf60568-cb81-4402-8bc6-fe228d72756b", "page_url": "https://www.thejewellers.com/gold-chain/9ct-white-gold-filed-curb-chain-necklace-c563838?sel=482236&gclid=CjwKCAiA9vS6BhA9EiwAJpnXw82wki6Z1QukUNffvjJo8a076Qc-3KPItDoGbkgsA6pp1q0bjkM0LRoC1WoQAvD_BwE", "page_referrer": "https://www.google.com/", "page_urlscheme": "https", "page_urlhost": "www.thejewellers.com", "page_urlport": 443, "page_urlpath": "/gold-chain/9ct-white-gold-filed-curb-chain-necklace-c563838", "page_urlquery": "sel=482236&gclid=CjwKCAiA9vS6BhA9EiwAJpnXw82wki6Z1QukUNffvjJo8a076Qc-3KPItDoGbkgsA6pp1q0bjkM0LRoC1WoQAvD_BwE", "refr_urlscheme": "https", "refr_urlhost": "www.google.com", "refr_urlport": 443, "refr_urlpath": "/", "refr_medium": "search", "refr_source": "Google", "contexts_com_snowplowanalytics_snowplow_web_page_1": [{"id": "33ac78a2-68cb-4423-8577-58c0a8548c15"}], "contexts_com_snowplowanalytics_snowplow_browser_context_1": [{"viewport": "1504x1000", "documentSize": "1504x3852", "resolution": "3008x1692", "colorDepth": 24, "devicePixelRatio": 2, "cookiesEnabled": True, "online": True, "browserLanguage": "en-GB", "documentLanguage": "en-gb", "webdriver": False, "deviceMemory": 8, "hardwareConcurrency": 10, "tabId": "db1e26f9-47f8-4fda-ab9b-9251fd75545c"}], "contexts_org_w3_performance_timing_1": [{"navigationStart": 1750431198930, "redirectStart": 0, "redirectEnd": 0, "fetchStart": 1750431198936, "domainLookupStart": 1750431198936, "domainLookupEnd": 1750431198936, "connectStart": 1750431198936, "secureConnectionStart": 0, "connectEnd": 1750431198936, "requestStart": 1750431198947, "responseStart": 1750431199921, "responseEnd": 1750431199928, "unloadEventStart": 1750431199963, "unloadEventEnd": 1750431199963, "domLoading": 1750431199977, "domInteractive": 1750431200009, "domContentLoadedEventStart": 1750431200105, "domContentLoadedEventEnd": 1750431200106, "domComplete": 1750431200456, "loadEventStart": 1750431200456, "loadEventEnd": 1750431200480}], "contexts_org_ietf_http_client_hints_1": [{"isMobile": False, "brands": [{"brand": "Google Chrome", "version": "137"}, {"brand": "Chromium", "version": "137"}, {"brand": "Not/A)Brand", "version": "24"}], "architecture": "arm", "model": "", "platform": "macOS", "uaFullVersion": "137.0.7151.104", "platformVersion": "15.5.0"}], "contexts_com_google_analytics_cookies_1": [{"_ga": "GA1.1.1197172085.1736957660"}], "contexts_co_hedingham_incognito_1": [{"incognito_enabled": False, "browser_name": "Chrome"}], "contexts_com_snowplowanalytics_snowplow_client_session_1": [{"userId": "1a289d33-ab98-46ac-88e4-ccb8caa912ee", "sessionId": "b4a7e900-31c5-4dfc-aac0-7e41f2bdad48", "eventIndex": 167, "sessionIndex": 83, "previousSessionId": "419dadec-c26b-44c0-b488-2fb80b7f1ad2", "storageMechanism": "COOKIE_1", "firstEventId": "9c0e67df-3171-46fe-8fd8-52009741f8a9", "firstEventTimestamp": "2025-06-20T13:13:54.258Z"}], "unstruct_event_com_snowplowanalytics_snowplow_ecommerce_snowplow_ecommerce_action_1": {"type": "list_view", "name": "Recommended Products"}, "useragent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36", "br_lang": "en-US", "br_cookies": True, "br_colordepth": "24", "br_viewwidth": 360, "br_viewheight": 667, "os_timezone": "Europe/London", "dvce_screenwidth": 360, "dvce_screenheight": 800, "doc_charset": "UTF-8", "doc_width": 360, "doc_height": 4725, "mkt_clickid": "CjwKCAiA9vS6BhA9EiwAJpnXw82wki6Z1QukUNffvjJo8a076Qc-3KPItDoGbkgsA6pp1q0bjkM0LRoC1WoQAvD_BwE", "mkt_network": "Google", "dvce_sent_tstamp": "2024-12-14T11:59:37.904Z", "contexts_com_snowplowanalytics_snowplow_ecommerce_product_1": [{"id": "1342", "name": "1oz Silver Britannia Coin Best Value", "url": "/silver-coins/britannia-silver-ounce/1oz-silver-britannia-best-value/", "position": 1, "currency": "GBP", "price": 37.32}, {"id": "62", "name": "Gold Britannia Coin 1oz Best Value 24ct", "url": "/gold-coins/britannia-1oz-gold-coin/britannia-1oz-gold-coin/", "position": 2, "currency": "GBP", "price": 2606}, {"id": "68", "name": "Gold Half Sovereign Best Value", "url": "/gold-coins/half-sovereign/bullion-half-sovereign/", "position": 3, "currency": "GBP", "price": 312.4}, {"id": "8718", "name": "2025 Gold Sovereign - Last of the Rose Gold", "url": "/gold-coins/full-sovereign-gold-coin/2025-gold-sovereign/", "position": 4, "currency": "GBP", "price": 618.5}], "contexts_com_snowplowanalytics_snowplow_ecommerce_page_1": [{"type": "product"}], "contexts_co_hedingham_client-id_1": [{"hedingham_client_id": "3a4449a1dab31e2944332e1a0862dda1"}], "contexts_uk_co_bullionbypost_hid_1": [{"hedingham_id": "5a5e9429-ce1e-4d74-ab06-924c1e04680e"}], "domain_sessionid": "3073b2c8-8a73-4e9d-a5a5-ca5075702e0d", "derived_tstamp": "2024-12-14T11:59:38.670Z", "event_vendor": "com.snowplowanalytics.snowplow", "event_name": "snowplow_ecommerce_action", "event_format": "jsonschema", "event_version": "1-0-0", "event_fingerprint": "d18d47f1ea3b7f9805f16c601c72b377", "True_tstamp": "\nTTZ"}


class TestUtils(unittest.TestCase):
  def test_flatten_event(self):
    self.assertEqual(flatten_event(data), expected_output)
  
  def test_unstruct_ecomm_event(self):
    self.assertEqual(flatten_event(unstruct_ecomm_event), expected_unstruct_ecomm_event)

  def test_unstruct_link_click_event(self):
    self.assertEqual(flatten_event(unstruct_link_click_event), expected_unstruct_link_click_event)

  def test_split_on_non_splitable_event(self):
    self.assertListEqual(split_event(data), [data])

  def test_split_on_list_view_event(self):
    self.assertEqual(len(split_event(list_view_event)), 4)

if __name__ == "__main__":
  unittest.main()
