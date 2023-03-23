    -- creates report exposing all levels of a bom
    -- *ADD Your projectid & datasetID in FROM clause*
    -- Create Temp Table Used in Loop
    -- prevents the need to unnest each pass
    BEGIN
      CREATE TEMPORARY TABLE tb AS
      SELECT 
        b.id bomID, 
        b.bom_name,
        o.product_code output,
        o.output_uom_name ouom,
        o.quantity oqty,
        i.product_code input,
        i.quantity,
        i.input_uom_name UoM,
        i.quantity_buildable_source,
        b.is_built_on_the_fly BotF
      FROM `projectid.datasetid.production_boms` b
      LEFT JOIN UNNEST(inputs) i
      LEFT JOIN UNNEST(outputs) o
      WHERE b.active = true
      ORDER BY o.product_code;
    END;
  -- Explode Boms Using Recursive With Loop
    WITH RECURSIVE RPL AS(
          SELECT 
            0 as level,
            ROOT.bomID,
            ROOT.bom_name,
            ROOT.output,
            ROOT.ouom,
            ROOT.oqty, 
            ROOT.input, 
            ROOT.quantity,
            ROOT.UoM,
            ROOT.quantity_buildable_source, 
            ROOT.BotF, 
            ROOT.output AS topSku, 
            ROOT.BotF AS topSkuBotF,
            ROOT.bom_name AS topBomName, 
            ROOT.bomID AS topBomID
          FROM tb ROOT 
          UNION ALL 
          SELECT 
            PARENT.level+1,
            CHILD.bomID,
            CHILD.bom_name, 
            CHILD.output,
            CHILD.ouom,
            CHILD.oqty, 
            CHILD.input, 
            CHILD.quantity, 
            CHILD.UoM,
            CHILD.quantity_buildable_source,  
            CHILD.BotF, 
            PARENT.topSku, 
            PARENT.topSkuBotF,
            PARENT.topBomName,
            PARENT.topBomID,
          FROM RPL PARENT, tb CHILD
          WHERE PARENT.input = CHILD.output
            AND PARENT.level < 10 -- incase of infinite loop, this breaks loop
    )
    -- Main Report
    SELECT DISTINCT  
      bomID,
      bom_name, 
      output,
      ouom output_UoM,
      oqty output_quantity, 
      level, 
      input, 
      quantity input_quantity, 
      UoM input_UoM,
      quantity_buildable_source, 
      BotF, 
      topSku, 
      topSkuBotF, 
      topBomName,
      topBomID,
      q.sequence
    FROM RPL
   -- Appends Bom Sequence  
    LEFT JOIN (
      SELECT
        b.bom_id,
        b.sequence
      FROM `projectid.datasetid.products`
      ,UNNEST(boms) b
    ) q ON q.bom_id = topBomID
    ORDER BY topSku, sequence, level, output;