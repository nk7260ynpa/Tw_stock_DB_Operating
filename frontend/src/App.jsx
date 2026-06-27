import { useState } from 'react'
import ManualUpload from './components/ManualUpload'
import ScheduleManager from './components/ScheduleManager'
import QuarterRevenueUpload from './components/QuarterRevenueUpload'
import TDCCUpload from './components/TDCCUpload'
import CTEENewsUpload from './components/CTEENewsUpload'
import CNYESNewsUpload from './components/CNYESNewsUpload'
import PTTNewsUpload from './components/PTTNewsUpload'
import MoneyUDNNewsUpload from './components/MoneyUDNNewsUpload'
import CompanyInfoUpload from './components/CompanyInfoUpload'
import RetryQueue from './components/RetryQueue'
import YTTranscriptUpload from './components/YTTranscriptUpload'
import OilPriceUpload from './components/OilPriceUpload'
import IndicesPriceUpload from './components/IndicesPriceUpload'

// 右側分頁設定：每個分頁含一個鍵值、顯示標籤與所屬卡片元件清單。
// 切換採 React 本地 state，未選到的分頁不掛載（條件渲染）以縮短初次載入。
const TABS = [
  {
    key: 'schedule',
    label: '排程/重試',
    components: [ScheduleManager, RetryQueue],
  },
  {
    key: 'market',
    label: '行情·公司',
    components: [QuarterRevenueUpload, TDCCUpload, CompanyInfoUpload],
  },
  {
    key: 'news',
    label: '新聞',
    components: [
      CTEENewsUpload,
      CNYESNewsUpload,
      PTTNewsUpload,
      MoneyUDNNewsUpload,
      YTTranscriptUpload,
    ],
  },
  {
    key: 'commodity',
    label: '商品·匯率·指數',
    components: [OilPriceUpload, IndicesPriceUpload],
  },
]

function App() {
  // 預設顯示第 1 個分頁「排程/重試」。
  const [activeTab, setActiveTab] = useState(TABS[0].key)

  const currentTab = TABS.find((tab) => tab.key === activeTab) ?? TABS[0]

  return (
    <div className="app">
      <header className="header">
        <h1>台股資料管理介面</h1>
        <p className="subtitle">Tw Stock DB Operating</p>
      </header>
      <main className="main">
        <ManualUpload />
        <div className="side-panels">
          <nav className="tab-bar" role="tablist" aria-label="資料管理分頁">
            {TABS.map((tab) => (
              <button
                key={tab.key}
                type="button"
                role="tab"
                aria-selected={tab.key === activeTab}
                className={`tab-btn${tab.key === activeTab ? ' active' : ''}`}
                onClick={() => setActiveTab(tab.key)}
              >
                {tab.label}
              </button>
            ))}
          </nav>
          <div className="tab-content" role="tabpanel">
            {currentTab.components.map((Component, index) => (
              // key 以「分頁鍵 + 索引」組成，避免依賴 minify 後會被改名的函式 name
              <Component key={`${currentTab.key}-${index}`} />
            ))}
          </div>
        </div>
      </main>
    </div>
  )
}

export default App
