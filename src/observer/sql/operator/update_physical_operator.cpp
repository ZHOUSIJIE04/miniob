#include "sql/operator/update_pyhsical_operator.h"
#include "sql/stmt/update_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "sql/operator/delete_physical_operator.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, Field field,Value value)
    : table_(table), value_(value),field_(field)
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}
RC UpdatePhysicalOperator::close()
{
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();
  
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &record = row_tuple->record();

  const char* field_name=field_.field_name();
  const FieldMeta* field_meta=table_->table_meta().field(field_name);

  int offset_=field_meta->offset();
  int len_=field_meta->len();

  rc=trx_->update_record(table_,record,offset_,len_,value_);

  if(rc!=RC::SUCCESS){
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
  }
  }
  return RC::RECORD_EOF;

}
