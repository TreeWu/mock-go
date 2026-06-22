DELETE FROM edge
WHERE source NOT IN (SELECT id FROM vertex)
   OR target NOT IN (SELECT id FROM vertex);
sk-6529b87692e4b342d8838947fab936a05a8d9a86dea71427da0b9902aa69a4a0

  http://tlu.dl.delivery.mp.microsoft.com/filestreamingservice/files/e8a5a9b1-ba5d-42d0-aa0b-bbedd74789e1?P1=1780916557&P2=404&P3=2&P4=cV19hxjaaGnPHYkSE99gHZ7bcoUHo8z7ts6qOQRdpKeB8LfQ%2bPOVlx1%2bf3mow2yfYYJedr%2bKmQj%2bkXmEa%2biFPg%3d%3d

{"parent":"project_root","scene":{"name":"0_1212","renderDsl":{"@type":"type.googleapis.com/xtwin.api.type.JSONContent","json":{"cfg":{},"group":{"row":[1],"col":[1],"list":[{"x1":0,"y1":0,"x2":1,"y2":1,"key":"XB000","view":"XB000"}]},"view":{"XB000":{"key":"XB000","layout":"XB000","cfg":{"customLayoutStyle":{"theme":"def_UE_schemalayout_theme_1"}}}},"instance":{"XB000":{"type":"HTPage","cfg":{"rid":"project_root","ciType":"1"}}},"layout":{"XB000":{"row":[1],"col":[1],"list":[{"x1":0,"y1":0,"x2":1,"y2":1,"key":"XB000","cfg":{}}]}}},"en_json":{"cfg":{},"group":{"row":[1],"col":[1],"list":[{"x1":0,"y1":0,"x2":1,"y2":1,"key":"XB000","view":"XB000"}]},"view":{"XB000":{"key":"XB000","layout":"XB000","cfg":{"customLayoutStyle":{"theme":"def_UE_schemalayout_theme_1"}}}},"instance":{"XB000":{"type":"HTPage","cfg":{"rid":"project_root","ciType":"1"}}},"layout":{"XB000":{"row":[1],"col":[1],"list":[{"x1":0,"y1":0,"x2":1,"y2":1,"key":"XB000","cfg":{}}]}}}},"displayName":"-"}}
http://193.3.62.200/xtwin/v3/metaverse/v1/scenes

  https://github.com/PowerShell/PowerShell/releases/download/v7.7.0-preview.2/PowerShell-7.7.0-preview.2-win-x64.zip

  https://github.com/farion1231/cc-switch/releases/download/v3.16.3/CC-Switch-v3.16.3-Windows.msi
  https://github.com/PowerShell/PowerShell/releases/download/v7.6.2/PowerShell-7.6.2-win-x64.msi


  导入结果
×
成功导入数据 0条，导入失败 1条
序号	数据位置	异常信息
1	第5行
从设备已存在主设备，从设备只能关联一个主设备，请先解绑原主从关系后再导入。
异常信息说明
说明：从设备编号允许为空，表示该主设备本次不建立主从关系，不计入异常。
主设备编号为空：数据页设备编号未填写，无法识别主设备。
设备编号不存在：主设备或从设备编号无法匹配到台账资产。
台账类型不匹配：模板带出的台账类型与实际设备来源不一致。
自关联：主设备编号与从设备编号相同。
单元格内重复：同一从设备编号在同一单元格内重复填写。
一从多主：从设备已存在主设备，需先解绑原关系。
超过两层：已是从设备的资产不能再作为主设备。
循环关系：导入后会形成 A→B→A 等闭环关系。
状态禁用：报废、待报废、迁出、废弃等状态禁止建立关系。
重复关系：导入关系与系统已有主从关系完全重复。
文件异常：文件格式错误、模板列缺失、解析失败或浏览器拒绝下载异常文件。