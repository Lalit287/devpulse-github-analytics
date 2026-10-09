import pandas as pd
import plotly.express as px
import streamlit as st
from dashboard import ui,service

# Existing local servers can retain the Week 5 service module between reruns.
if not hasattr(service,'model_experiment'):
    import importlib
    importlib.reload(service)

ui.heading('Popularity prediction','Seven-day repository attention growth — evaluated historical experiment.')
experiment=service.model_experiment()
if experiment is None:
    st.info('No evaluated prediction model is available yet. Week 6 will add predictions and measured model results.')
    st.markdown('A model requires complete historical feature/outcome windows and evaluation on later periods.')
    st.stop()
model_id=experiment['model_id'];e=experiment['evaluation'];chosen=e['selected_model'];test=e['models'][chosen]['test']
st.warning('Historical experiment: January–February 2015. These scores forecast the week following the historical cutoff; they are not current GitHub recommendations. The activity pages use the separate 2025 dataset.')
st.sidebar.caption('Prediction experiment: January–February 2015')
algorithm_names={'logistic_regression':'Logistic regression','random_forest':'Random forest','gradient_boosted_trees':'Gradient boosting'}
ui.cards([('Selected algorithm',algorithm_names[chosen],'Selected using validation PR-AUC, before inspecting test results.'),
          ('Held-out PR-AUC',f"{test['pr_auc']:.3f}",'Spark trapezoidal area under the precision-recall curve.'),
          ('Precision @ 10',f"{test['precision_at_10']:.0%}",'Positive fraction among the ten highest test scores.'),
          ('Target prevalence',f"{test['prevalence']:.2%}",'Positive fraction across all eligible test repositories.')])
st.caption(f"Target: at least 5 observed future star actions and a gain of at least {e['target_growth_threshold']} versus the previous seven days. The gain threshold is derived from training data only.")
if test['pr_auc']<=e['baseline']['test']['pr_auc']:
    st.info('The selected model did not beat the simple recent-star ranking baseline on test PR-AUC. Treat the result as an evaluated baseline experiment requiring further improvement.')
st.subheader('Historical forecast rankings')
left,right=st.columns([3,1])
with left:search=st.text_input('Repository name contains',max_chars=150,key='prediction_search')
with right:limit=st.selectbox('Rows to show',[10,20,50,100],index=1,key='prediction_limit')
rows=service.model_forecasts(model_id,search,limit)
rows['predicted_trending']=rows['predicted_probability']>=e['decision_threshold']
st.caption(f"Forecast cutoff: {e['splits']['forecast']['last_as_of']} UTC · Horizon: 7 days · Scores are uncalibrated model probabilities; actual outcomes for this forecast are not included in the corpus.")
forecast_display=rows[['repo_name','predicted_probability','predicted_trending','recent_stars','recent_forks',
                       'active_accounts','recent_events','repo_id','as_of_utc','horizon_days','recent_pushes']].rename(
    columns={'repo_name':'Repository','predicted_probability':'Model score','predicted_trending':'Predicted trending'})
ui.show_table(forecast_display,'prediction_csv','devpulse_historical_forecasts.csv')
if not rows.empty:ui.bar(rows.head(10),'predicted_probability','repo_name','Highest historical model scores',horizontal=True)
st.subheader('Actual versus predicted — held-out test period')
heldout=service.heldout_predictions(model_id,100)
heldout['predicted_trending']=heldout['predicted_probability']>=e['decision_threshold']
heldout_display=heldout[['repo_name','predicted_probability','actual_label','predicted_trending',
                         'past_stars','future_stars','repo_id','as_of_utc']].rename(
    columns={'repo_name':'Repository','predicted_probability':'Model score','actual_label':'Actual trending',
             'predicted_trending':'Predicted trending'})
ui.show_table(heldout_display,'heldout_csv','devpulse_heldout_predictions.csv')
st.caption('This table contains the top 100 test scores, not the full evaluation population. Metrics below use every eligible test example.')
st.subheader('Model comparison')
comparison=[]
for name,result in e['models'].items():
    for split in ['validation','test']:
        comparison.append({'Algorithm':name,'Split':split,**result[split]})
for split,result in e['baseline'].items():comparison.append({'Algorithm':'recent_star_ranking_baseline','Split':split,**result})
st.dataframe(pd.DataFrame(comparison),hide_index=True,width='stretch')
st.caption('Algorithms and decision thresholds are selected on validation only. Test precision, recall and F1 use the frozen validation threshold. Brier score and the reliability chart assess uncalibrated probability quality.')
left,right=st.columns(2)
with left:
    st.subheader('Test confusion matrix')
    matrix=pd.DataFrame([[test['tn'],test['fp']],[test['fn'],test['tp']]],index=['Actual negative','Actual positive'],columns=['Predicted negative','Predicted positive'])
    st.plotly_chart(px.imshow(matrix,text_auto=True,color_continuous_scale='Teal',aspect='auto'),width='stretch')
with right:
    st.subheader('Test score reliability')
    reliability=pd.DataFrame(e['models'][chosen]['test_diagnostics']['reliability_bins'])
    figure=px.scatter(reliability,x='mean_score',y='observed_positive_fraction',size='rows',template='plotly_white')
    figure.add_shape(type='line',x0=0,y0=0,x1=1,y1=1,line=dict(dash='dash',color='gray'))
    figure.update_xaxes(range=[0,1],title='Mean predicted probability');figure.update_yaxes(range=[0,1],title='Observed positive fraction')
    st.plotly_chart(figure,width='stretch')
with st.expander('Reproducibility and limitations'):
    st.json({'model_id':model_id,'corpus_id':e['corpus']['corpus_id'],'source_hours':e['corpus']['hours'],
             'unique_events':e['clean_events'],'splits':e['splits'],'feature_contract':e['feature_contract'],
             'checks':e['checks'],'decision_threshold':e['decision_threshold']})
    st.markdown('Six consecutive weeks provide only one validation and one test period. Stable repository IDs may recur across time; this evaluates later activity for observed repositories, not unseen-repository generalization. Current metadata, repository IDs/names and future counts are excluded from model inputs. Archive hours approximate historical observation availability; publication latency is not reconstructed. Age and 30-day features are omitted because those histories are incomplete. Training on 2015 data does not establish performance on 2025 or current GitHub activity.')
