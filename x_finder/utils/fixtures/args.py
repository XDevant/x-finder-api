
item_category_arguments = {"default": {"app": "utils",
                                       "item_url_column": 'Name',
                                       "text_columns": [],
                                       "url_columns": [],
                                       "row_separator": '<br/><b>',
                                       "start": '</h1><b>',
                                       "end": '<h2',
                                       "cell_separator": '</b>',
                                       "tail_start": '<br>'
                                       },
                           "sources": {"app": "core",
                                       "text_columns": ["Latest Errata"],
                                       "url_columns": ["Product Page", "Latest Errata"],
                                       },
                           "traits": {"app": "core",
                                      "text_columns": ["Source", "Other"],
                                      "tail_start": '</sup><br/>',
                                      "subtype": True
                                      }
                           }
