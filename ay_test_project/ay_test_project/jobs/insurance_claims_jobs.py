# ay_test_project/ay_test_project/jobs/insurance_claims_job.py
from dagster import job, op, Config, OpExecutionContext, RunRequest, ScheduleEvaluationContext, schedule
from dagster import DefaultScheduleStatus
from datetime import datetime
import pytz


class InsuranceClaimsJobConfig(Config):
    """Configuration for the insurance claims processing job"""
    max_batch_size: int = 100
    max_retries: int = 3
    dry_run: bool = False  # Set to True to simulate without actual updates


@job(
    name="insurance_claims_processing",
    description="Process QuickBase insurance claims and update Smartsheet tracker",
    config=InsuranceClaimsJobConfig,
    tags={"pipeline": "insurance_claims", "source": "quickbase", "destination": "smartsheet"}
)
def insurance_claims_job():
    """
    Complete insurance claims processing pipeline:
    1. Load field mappings and configurations
    2. Extract active claims from QuickBase  
    3. Get current Smartsheet state
    4. Extract QB data for all claims
    5. Create processing plan (updates vs additions)
    6. Execute batch updates for existing claims
    7. Execute batch additions for new claims  
    8. Add subtasks to new claims
    9. Generate processing summary
    """
    from ay_test_project.assets.insurance_claim_tracking import (
        field_mapping_config,
        active_qb_claims,
        current_smartsheet_data,
        qb_claim_data,
        claim_processing_plan,
        updated_smartsheet_claims,
        new_smartsheet_claims,
        template_sheet_data,
        claim_subtasks,
        processing_summary
    )
    
    # Build dependency chain
    field_mappings = field_mapping_config()
    qb_claims = active_qb_claims()
    sheet_data = current_smartsheet_data()
    
    qb_data = qb_claim_data(qb_claims, sheet_data, field_mappings)
    processing_plan = claim_processing_plan(qb_data, sheet_data)
    
    # Process updates and additions in parallel where possible
    updates = updated_smartsheet_claims(processing_plan, qb_data, field_mappings)
    additions = new_smartsheet_claims(processing_plan, qb_data, field_mappings, updates)
    
    # Template data can be loaded in parallel
    template_data = template_sheet_data()
    
    # Add subtasks to new claims
    subtasks = claim_subtasks(additions, template_data)
    
    # Generate final summary
    summary = processing_summary(updates, additions, subtasks)
    
    return summary


@schedule(
    cron_schedule="0 7 * * *",  # Daily at 7 AM
    job=insurance_claims_job,
    execution_timezone="US/Central",
    default_status=DefaultScheduleStatus.STOPPED,  # Start manually
)
def daily_insurance_claims_schedule(context: ScheduleEvaluationContext):
    """
    Daily schedule for insurance claims processing
    Runs every morning at 7 AM Central Time
    """
    return RunRequest(
        run_key=f"insurance_claims_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M%S')}",
        tags={
            "schedule": "daily_insurance_claims",
            "execution_date": context.scheduled_execution_time.strftime('%Y-%m-%d'),
        }
    )


@schedule(
    cron_schedule="0 */6 * * *",  # Every 6 hours
    job=insurance_claims_job,
    execution_timezone="US/Central", 
    default_status=DefaultScheduleStatus.STOPPED,
)
def frequent_insurance_claims_schedule(context: ScheduleEvaluationContext):
    """
    Frequent schedule for insurance claims processing
    Runs every 6 hours for more up-to-date tracking
    """
    return RunRequest(
        run_key=f"insurance_claims_frequent_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M%S')}",
        tags={
            "schedule": "frequent_insurance_claims", 
            "execution_date": context.scheduled_execution_time.strftime('%Y-%m-%d_%H:%M'),
        }
    )