import re
pattern = re.compile(r'(?<!^)(?=[A-Z])')


def convert_context_name(context_name):
  conversions={
    'contexts_ai_pragmatik_ip-api_1': 'ip_api',
    'contexts_ai_pragmatik_incognito_1': 'incognito',
    'contexts_ai_pragmatik_hid_1': 'hid',
    'contexts_ai_pragmatik_client_id_1': 'client_id',
    'contexts_ai_pragmatik_client-id_1': 'client_id',
    'contexts_co_hedingham_ip-api_1': 'ip_api',
    'contexts_co_hedingham_incognito_1': 'incognito',
    'contexts_co_hedingham_hid_1': 'hid',
    'contexts_co_hedingham_client_id_1': 'client_id',
    'contexts_co_hedingham_client-id_1': 'client_id',
    'contexts_uk_co_bullionbypost_hid_1': 'bbp_hid',
    'contexts_com_snowplowanalytics_snowplow_web_page_1': 'web_page',
    'contexts_com_snowplowanalytics_snowplow_browser_context_1': 'browser_context',
    'contexts_org_w3_performance_timing_1': 'performance_timing',
    'contexts_org_ietf_http_client_hints_1': 'http_client_hints',
    'contexts_com_google_analytics_cookies_1': 'ga_cookies',
    'contexts_com_snowplowanalytics_snowplow_client_session_1': 'client_session',
    'contexts_com_snowplowanalytics_snowplow_ua_parser_context_1': 'ua',
    'contexts_com_iab_snowplow_spiders_and_robots_1': 'spiders_and_robots',
    'contexts_com_snowplowanalytics_snowplow_ecommerce_cart_1': 'ecommerce_cart',
    'contexts_com_snowplowanalytics_snowplow_ecommerce_checkout_step_1': 'ecommerce_checkout_step',
    'contexts_com_snowplowanalytics_snowplow_ecommerce_page_1': 'ecommerce_page',
    'contexts_com_snowplowanalytics_snowplow_ecommerce_product_1': 'ecommerce_product',
    'contexts_com_snowplowanalytics_snowplow_ecommerce_transaction_1': 'ecommerce_transaction',
    'contexts_com_snowplowanalytics_snowplow_ecommerce_user_1': 'ecommerce_user',
    'contexts_nl_basjes_yauaa_context_1': 'yauaa',
    'unstruct_event_com_snowplowanalytics_snowplow_ecommerce_snowplow_ecommerce_action_1': 'ecommerce_action',
    'unstruct_event_com_snowplowanalytics_snowplow_link_click_1': 'link_click',
    'unstruct_event_com_snowplowanalytics_snowplow_submit_form_1': 'submit_form'
  }
  if context_name not in conversions:
    return f'unknown_{context_name.replace("contexts_", "").replace("_1", "").replace("unstruct_event_", "")}'
  return conversions[context_name]


def convert_context(context_name, context_data):
  result ={}
  context_key = convert_context_name(context_name)
  for data in context_data:
    for key in data:
      fmt_key = pattern.sub('_', key).lower()
      result[f"{context_key}_{fmt_key}"] = data[key]
    break # only want to process the first item in the list
  return result

def convert_unstruct_event(context_name, context_data):
  result ={}
  context_key = convert_context_name(context_name)
  for key in context_data:
    fmt_key = pattern.sub('_', key).lower()
    result[f"{context_key}_{fmt_key}"] = context_data[key]
  return result

def flatten_event(event):
  result ={}
  for key in event:
    if key.startswith('contexts_'):
      c = convert_context(key, event[key])
      result = {**result, **c}
    elif key.startswith('unstruct_event_'):
      c = convert_unstruct_event(key, event[key])
      result = {**result, **c}
    else:
      result[key] = event[key]
  return result


def split_event(event):
  """
  Split the event into multiple events based on the unstruct_event_ schema.
  Specifically, when unstruct_event_ schema is of unstruct_event_com_snowplowanalytics_snowplow_ecommerce_snowplow_ecommerce_action_1 and the action type is list_view,
  we need to split the event into multiple events.
  """
  results = []
  has_split =False
  for key in event:
    if 'ecommerce_action' in key and event[key]['type'] == 'list_view':
      for product in event['contexts_com_snowplowanalytics_snowplow_ecommerce_product_1']:
        has_split = True
        new_event = event.copy()
        new_event['contexts_com_snowplowanalytics_snowplow_ecommerce_product_1'] = [product]
        results.append(new_event)
  if not has_split:
    results.append(event)
  return results