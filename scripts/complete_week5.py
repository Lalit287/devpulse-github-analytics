"""Run in normal Mac Terminal to finish native PostgreSQL loading and verification."""
from config.settings import ROOT
from database.cluster import start
from database.load_analytics import load_analytics
from dashboard.run import start as start_dashboard
from exploration.io import write_json
from scripts.week5_evidence import EVIDENCE, TEST_GROUPS, run_tests


def main():
    from scripts.generate_week5_report import generate
    from scripts.verify_week5 import verify
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    try:
        # Prevent old success from surviving an interrupted or failed rerun.
        write_json(EVIDENCE / 'verification.json', {'status': 'running'})
        print('Starting the isolated project PostgreSQL cluster...', flush=True)
        print(start(), flush=True)
        print('Loading the verified real Gold dataset...', flush=True)
        result = load_analytics()
        write_json(EVIDENCE / ('initial_load.json' if result['status'] == 'complete' else 'reused_load.json'), result)
        result = load_analytics()
        if result['status'] != 'reused' or result['copied_rows'] != 0:
            raise RuntimeError('Unchanged rerun unexpectedly copied data')
        write_json(EVIDENCE / 'idempotent_load.json', result)
        print(start_dashboard(), flush=True)
        for name in TEST_GROUPS:
            print(f'Running {name}...', flush=True)
            run_tests(name)
            print((EVIDENCE / f'{name}.log').read_text().strip(), flush=True)
        verify()
    except BaseException as exc:
        write_json(EVIDENCE / 'verification.json', {'status': 'failed', 'error_class': type(exc).__name__})
        generate()
        raise
    if not generate():
        raise RuntimeError('Completion evidence is stale or incomplete')
    print('Week 5 complete. Dashboard: http://127.0.0.1:18501', flush=True)
    print('Evidence: reports/week5_report.md', flush=True)


if __name__ == '__main__':
    main()
