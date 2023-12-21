from include.common.constants.index import SLACK_CONN_ID
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

SLACK_CHANNEL='#spock-jenkins-notification'
ICON_URL='https://res.cloudinary.com/dalayuyv1/image/upload/v1702479633/download_l0fg4f.png'

def notify_success(context, task_id='notify_success'):
    attachments = [{
        'text': (
            f'''
            Task Successful >> Protocol: {context.get('task_instance').dag_id} >> CC: <@U02DZLLR01L>, <@U01QFUN1F2N>, <@USXTDMWEN>
            '''
            ),
        'color': '#90EE90'
    }]
    _notify = SlackAPIPostOperator(
        task_id=task_id,
        slack_conn_id=SLACK_CONN_ID,
        channel=SLACK_CHANNEL,
        icon_url=ICON_URL,
        attachments=attachments,
        text=''
    )
    return _notify.execute(context=context)


def notify_failure(context, task_id='failure_notification'):
    attachments = [{
        'text': (
            f'''
            Task Failed >> Protocol: {context.get('task_instance').dag_id} >> CC: <@U02DZLLR01L>, <@U01QFUN1F2N>, <@USXTDMWEN>
            '''
            ),
        'color': '#FF474C'
    }]
    _notify = SlackAPIPostOperator(
        task_id=task_id,
        slack_conn_id=SLACK_CONN_ID,
        channel=SLACK_CHANNEL,
        icon_url=ICON_URL,
        attachments=attachments,
        text=''
    )
    return _notify.execute(context=context)