var dom = document.getElementById("container-c2pt");
var myChart = echarts.init(dom, null, {renderer: 'svg'});
var app = {};

var option;

option = {
    // title: {
    //    text: '基础雷达图'
    // },
    toolbox: {
        show: true,
        feature: {
          dataZoom: {
            yAxisIndex: "none"
          },
          dataView: {
            readOnly: false
          },
          magicType: {
            type: ["line", "bar"]
          },
          restore: {},
          saveAsImage: {
              type: "svg"
          }
        }
    },
    color: [
        "#FF0000", "#5B60D8"
    ],
    textStyle: {
        color: "black",
        fontSize: 20
    },
    legend: {
        data: ['IRA', 'non-IRA'],
        top: 40
    },
    radar: {
        // shape: 'circle',
        indicator: [
            { name: 'fake', max: 0.3},
            { name: 'extreme bias (right)', max: 0.3},
            { name: 'right', max: 0.3},
            { name: 'right leaning', max: 0.3},
            { name: 'center', max: 0.3},
            { name: 'left leaning', max: 0.3},
            { name: 'left', max: 0.3},
            { name: 'extreme bias (left)', max: 0.3},
        ],
        axisLabel: {
            show: true
        },
    },
    series: [{
        type: 'radar',
        data: [
            {
                value: [0.03, 0.04, 0.06, 0.03, 0.23, 0.31, 0.26, 0.05],
                name: 'non-IRA',
                areaStyle: {},
            },
            {
                value: [0.04, 0.01, 0.02, 0.15, 0.25, 0.31, 0.21, 0.01],
                name: 'IRA',
                areaStyle: {},
            },

        ]
    }]
};

if (option && typeof option === 'object') {
    myChart.setOption(option);
}
