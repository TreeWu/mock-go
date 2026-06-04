DELETE FROM edge
WHERE source NOT IN (SELECT id FROM vertex)
   OR target NOT IN (SELECT id FROM vertex);


{"parent":"project_root","scene":{"name":"0_1212","renderDsl":{"@type":"type.googleapis.com/xtwin.api.type.JSONContent","json":{"cfg":{},"group":{"row":[1],"col":[1],"list":[{"x1":0,"y1":0,"x2":1,"y2":1,"key":"XB000","view":"XB000"}]},"view":{"XB000":{"key":"XB000","layout":"XB000","cfg":{"customLayoutStyle":{"theme":"def_UE_schemalayout_theme_1"}}}},"instance":{"XB000":{"type":"HTPage","cfg":{"rid":"project_root","ciType":"1"}}},"layout":{"XB000":{"row":[1],"col":[1],"list":[{"x1":0,"y1":0,"x2":1,"y2":1,"key":"XB000","cfg":{}}]}}},"en_json":{"cfg":{},"group":{"row":[1],"col":[1],"list":[{"x1":0,"y1":0,"x2":1,"y2":1,"key":"XB000","view":"XB000"}]},"view":{"XB000":{"key":"XB000","layout":"XB000","cfg":{"customLayoutStyle":{"theme":"def_UE_schemalayout_theme_1"}}}},"instance":{"XB000":{"type":"HTPage","cfg":{"rid":"project_root","ciType":"1"}}},"layout":{"XB000":{"row":[1],"col":[1],"list":[{"x1":0,"y1":0,"x2":1,"y2":1,"key":"XB000","cfg":{}}]}}}},"displayName":"-"}}
http://193.3.62.200/xtwin/v3/metaverse/v1/scenes