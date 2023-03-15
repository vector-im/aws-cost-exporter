select
    (`bill_BillingPeriodStartDate` || "-" || `bill_BillingPeriodEndDate`)  as `period`,

    `product_ProductName` as `product`,
    `lineItem_Operation` as `operation`,
    `lineItem_LineItemType` as `item_type`,

    `lineItem_UsageType` as `usage_type`,
    `pricing_unit` as `usage_unit`,
    SUM(`lineItem_UsageAmount`) as metric_amount,

    SUM(`lineItem_UnblendedCost`) as metric_cost,
    `lineItem_CurrencyCode` as `currency`,
from `records`
where `lineItem_UnblendedCost` > 0
group by
    `bill_BillingPeriodStartDate`,
    `bill_BillingPeriodEndDate`,
    `product_ProductName`,
    `lineItem_Operation`,
    `lineItem_LineItemType`,

    `lineItem_UsageType`,
    `pricing_unit`,
    `lineItem_CurrencyCode`
order by `period`, `product`, `operation`
