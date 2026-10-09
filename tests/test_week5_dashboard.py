"""Exercise real database-backed Streamlit pages and their interactive states."""
import pytest
from streamlit.testing.v1 import AppTest
from config.settings import ROOT


@pytest.fixture
def app():
    return AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=30).run()


def test_overview_real_kpis(app):
    assert not app.exception
    assert app.title[0].value=='GitHub activity overview'
    assert any(m.value=='3,909,986' for m in app.metric)


def test_repository_filters_empty_state_comparison(app):
    app.switch_page('pages/repositories.py').run()
    assert not app.exception and len(app.dataframe)>=1
    app.selectbox(key='repo_sort').set_value('Intraday score').run()
    assert not app.exception
    app.multiselect(key='repo_compare').set_value([]).run()
    assert any('at least two' in i.value for i in app.info)
    app.text_input(key='repo_search').set_value('missing-repo__verification__').run()
    assert not app.exception and any('No matching' in i.value for i in app.info)
    app.text_input(key='repo_search').set_value('').run()
    app.selectbox(key='repo_language').set_value('Rust').run()
    assert not app.exception and set(app.dataframe[0].value['primary_language'])=={'Rust'}


def test_language_modes_and_coverage(app):
    app.switch_page('pages/languages.py').run()
    assert not app.exception
    app.radio(key='language_mode').set_value('Current code-byte allocation').run()
    assert not app.exception
    app.checkbox(key='language_unknown').set_value(True).run()
    assert not app.exception and 'Not enriched' in set(app.dataframe[0].value['language'])


def test_public_accounts_search_and_network(app):
    app.switch_page('pages/accounts.py').run()
    assert not app.exception
    app.text_input(key='account_search').set_value('github-actions[bot]').run()
    assert not app.exception and len(app.dataframe[0].value)>=1
    app.text_input(key='account_search').set_value('missing-account__verification__').run()
    assert not app.exception and any('No matching' in i.value for i in app.info)


def test_predictions_not_fabricated_and_monitoring(app):
    app.switch_page('pages/predictions.py').run()
    assert not app.exception
    from dashboard import service
    if service.model_experiment() is None:
        assert any('No evaluated' in i.value for i in app.info)
    else:
        assert any('Historical experiment' in i.value for i in app.warning)
    app.switch_page('pages/pipeline.py').run()
    assert not app.exception and any(m.value=='devpulse_reader' for m in app.metric)
