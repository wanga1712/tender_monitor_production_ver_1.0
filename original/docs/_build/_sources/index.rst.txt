.. TenderMonitor documentation master file, created by
   sphinx-quickstart on Fri Feb 28 16:49:09 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

TenderMonitor documentation
===========================

Add your content using ``reStructuredText`` syntax. See the
`reStructuredText <https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html>`_
documentation for details.


.. toctree::
   :maxdepth: 2
   :caption: Содержание:

   modules


.. currentmodule:: database_work.check_database

.. autoclass:: DatabaseCheckManager
    :members:
    :undoc-members:
    :show-inheritance:

.. autofunction:: __init__
.. autofunction:: get_db_manager
.. autofunction:: close
.. autofunction:: check_okpd_in_db
.. autofunction:: is_file_name_recorded
.. autofunction:: check_inn_and_get_id_customer


.. currentmodule:: database_work.database_connection

.. autoclass:: DatabaseManager
    :members:
    :undoc-members:
    :show-inheritance:

.. autofunction:: __init__
.. autofunction:: execute_query
.. autofunction:: fetch_one
.. autofunction:: close
.. autofunction:: is_file_name_recorded
.. autofunction:: check_inn_and_get_id_customer

.. currentmodule:: database_work.database_id_fetcher

.. autoclass:: DatabaseIDFetcher
    :members:
    :undoc-members:
    :show-inheritance:

.. autofunction:: __init__
.. autofunction:: get_connection
.. autofunction:: fetch_id
.. autofunction:: get_collection_codes_okpd_id
.. autofunction:: get_contractor_id
.. autofunction:: get_customer_id
.. autofunction:: get_dates_id
.. autofunction:: get_file_names_xml_id
.. autofunction:: get_key_words_names_id
.. autofunction:: get_key_words_names_documentations_id
.. autofunction:: get_links_documentation_223_fz_id
.. autofunction:: get_links_documentation_44_fz_id
.. autofunction:: get_okpd_from_users_id
.. autofunction:: get_reestr_contract_223_fz_id
.. autofunction:: get_reestr_contract_44_fz_id
.. autofunction:: get_region_id
.. autofunction:: get_stop_words_names_id
.. autofunction:: get_trading_platform_id
.. autofunction:: get_users_id
.. autofunction:: get_users_id

.. currentmodule:: database_work.database_operations


.. autoclass:: DatabaseOperations
    :members:
    :undoc-members:
    :show-inheritance:

.. autofunction:: __init__
.. autofunction:: insert_customer
.. autofunction:: _update_field

.. currentmodule:: database_work.database_requests

.. autofunction:: get_region_codes