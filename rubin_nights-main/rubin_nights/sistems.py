config_with_interval = [
    {'name': 'Dome Shutter', 'measurement': 'lsst.sal.MTDome.apertureShutter', 'field': 'subpositionActual0tate+positionActual1', 'asset_id': '502539', 'attribute': 'AC_count', 'db_name': 'efd', 'time_interval': '24h'},
    {'name': 'Use Hours Generador 750 kva', 'measurement': 'lsst.sal.ESS.agcGenset150', 'field': 'engineHours', 'asset_id': '502673', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '24h', 'salIndex': 305},
    {'name': 'Use hours generator 1100kva', 'measurement': 'lsst.sal.ESS.agcGenset150', 'field': 'engineHours', 'asset_id': '502624', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '24h', 'salIndex': 306},
    {'name': 'Use Hours compressor 1', 'measurement': 'lsst.sal.MTAirCompressor.logevent_timerInfo', 'field': 'lowestServiceCounter', 'asset_id': '431482', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '30d', 'salIndex': 1},
    {'name': 'Use Hour air compressor 2', 'measurement': 'lsst.sal.MTAirCompressor.logevent_timerInfo', 'field': 'lowestServiceCounter', 'asset_id': '431489', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '2d', 'salIndex': 2},
    {'name': 'MTcamera Chiller tank level', 'measurement': 'lsst.MTCamera.chiller', 'field': 'TankLevel', 'asset_id': '502819', 'attribute': 'NoiseLevel', 'db_name': 'lsst.MTCamera', 'time_interval': '30h'},
    {'name': 'glycol flow', 'measurement': 'lsst.sal.HVAC.glycolSensor', 'field': 'supplyFlowChiller03', 'asset_id': '502903', 'attribute': 'NoiseLevel', 'db_name': '', 'time_interval': ''},
    {'name': 'MTcamera cryo oil leve', 'measurement': 'lsst.MTCamera.refrig_cryo', 'field': 'OilLevel', 'asset_id': '503018', 'attribute': 'NoiseLevel', 'db_name': 'lsst.MTCamera', 'time_interval': '24h'},
    {'name': 'Use Hours cleanRoomAHU01P05', 'measurement': 'lsst.sal.HVAC.cleanRoomAHU01P05', 'field': 'hourMeasure', 'asset_id': '503164', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '24h'},
    {'name': 'Use Hours whiteRoomAHU01P05\n', 'measurement': 'lsst.sal.HVAC.whiteRoomAHU01P05', 'field': 'hourMeasure', 'asset_id': '503286', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '24h'},
    {'name': 'Compresor use hours chiller01P01', 'measurement': 'lsst.sal.HVAC.chiller01P01', 'field': 'averageCompressorHours', 'asset_id': '503349', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '24h'},
    {'name': 'Compresor use hours chiller02P01', 'measurement': 'lsst.sal.HVAC.chiller02P01', 'field': 'averageCompressorHours', 'asset_id': '503402', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '24h'},
    {'name': 'Compresor use hours chiller03P01', 'measurement': 'lsst.sal.HVAC.chiller03P01', 'field': 'averageCompressorHours', 'asset_id': '503476', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '24h'},
    {'name': 'Compresor use hours chiller04P01', 'measurement': 'lsst.sal.HVAC.chiller04P01', 'field': 'averageCompressorHours', 'asset_id': '503515', 'attribute': 'NoiseLevel', 'db_name': 'efd', 'time_interval': '24h'},
]
