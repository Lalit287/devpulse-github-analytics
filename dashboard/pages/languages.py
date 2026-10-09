import streamlit as st
from dashboard import ui

s=ui.snapshot();coverage=s['facts']['coverage']
ui.heading('Languages & technology','Archived activity associated with selected current repository metadata.')
ui.cards([('Repositories enriched',f"{coverage['successful_repositories']} / {coverage['selected_repositories']}",'Complete public metadata with the same archived repository ID.'),
          ('Metadata event coverage',f"{coverage['covered_event_fraction']*100:.2f}%",'Targeted selection, not a representative GitHub sample.'),
          ('Unavailable selections',str(coverage['selected_repositories']-coverage['successful_repositories']),'Includes not-found repositories and identity mismatches.')])
start,end=ui.date_filter(s,'language_dates')
mode=st.radio('Language association',['Current primary language','Current code-byte allocation'],horizontal=True,key='language_mode')
include_unknown=st.checkbox('Include unenriched and unavailable buckets',key='language_unknown')
weighted=mode=='Current code-byte allocation'
rows=ui.data(s,'languages',start=start,end=end,weighted=weighted)
column='language' if weighted else 'primary_language'
metric='attributed_total_events' if weighted else 'total_events'
named=rows[~rows[column].isin(['Not enriched','Metadata unavailable','No primary language reported','No language reported'])]
named_events=float(named[metric].sum())
total=float(rows[metric].sum())
st.caption(f'Named associations cover {named_events:,.0f} event equivalents ({named_events/total*100 if total else 0:.2f}% of this date selection).')
shown=rows if include_unknown else named
ui.show_table(shown,'language_csv','devpulse_language_associations.csv')
ui.bar(shown.groupby(column,as_index=False)[metric].sum().sort_values(metric,ascending=False).head(20),column,metric,'Language-associated activity')
st.info('Metadata was collected after the archived activity. Current languages and code-byte mixes may differ from historical languages; byte allocation estimates associations, not the language of each event.')
st.subheader('Equal-period attention by current language')
growth=ui.data(s,'language_growth')
if not include_unknown:growth=growth[~growth['primary_language'].isin(['Not enriched','Metadata unavailable','No primary language reported'])]
st.dataframe(growth,hide_index=True,width='stretch')
st.caption('Growth uses the fixed equal-period comparison for the complete loaded window.')
st.subheader('Technology associations')
topics=ui.data(s,'technologies',start=start,end=end)
if not include_unknown:topics=topics[topics['technology_category']!='Not enriched']
ui.bar(topics,'technology_category','total_events','Activity associated with topic/description categories')
st.caption('Categories can overlap. Their counts cannot be summed into a global total.')
