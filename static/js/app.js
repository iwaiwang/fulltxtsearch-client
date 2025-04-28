// 配置 PDF.js 的 workerSrc
if (typeof pdfjsLib !== 'undefined') {
    pdfjsLib.GlobalWorkerOptions.workerSrc = '/static/pdfjs/build/pdf.worker.js';
} else {
    console.error('pdfjsLib is not loaded');
}

new Vue({
    el: '#app',
    data: {
        query: '',
        showFilters: false,
        filters: {
            patientName: '',
            hospitalId: '',
            admissionDateStart: '',
            admissionDateEnd: '',
            dischargeDateStart: '',
            dischargeDateEnd: '',
            fileType: ''
        },
        fileTypes: [],
        results: [],
        pdfUrl: null,
        pdfPage: 1,
        totalPdfPages: 1,
        total: 0,
        current_page: 1,
        size: 10,
        total_pages: 0,
        error: '',
        pdfDocument: null,
        debugHighlight: true, // 调试模式，显示高亮边框

        // New WebDAV settings data
        showSettingsModal: false,
        webdavSettings: {
            ip: '',
            port: 0,
            user: '',
            password: '',
            directory: '',
            enabled: false
        },
        pdfcanvas: null,
        pdfcanvasContext: null,

    },
    computed: {
        pageRange() {
            const range = [];
            const maxPages = 5;
            let start = Math.max(1, this.current_page - Math.floor(maxPages / 2));
            let end = Math.min(this.total_pages, start + maxPages - 1);

            if (end - start < maxPages - 1) {
                start = Math.max(1, end - maxPages + 1);
            }

            for (let i = start; i <= end; i++) {
                range.push(i);
            }
            return range;
        }
    },
    mounted() {
        // 加载文件类型
        axios.get('/api/file_types')
            .then(response => {
                this.fileTypes = response.data;
            })
            .catch(error => {
                console.error('Failed to load file types:', error);
            });
    },
    methods: {
        toggleFilters() {
            this.showFilters = !this.showFilters;
            if (!this.showFilters) {
                // 收起时清空过滤条件
                this.filters = {
                    patientName: '',
                    hospitalId: '',
                    admissionDateStart: '',
                    admissionDateEnd: '',
                    dischargeDateStart: '',
                    requestedDateEnd: '', // Corrected typo? assuming dischargeDateEnd was intended
                    dischargeDateEnd: '', // Corrected typo?
                    fileType: ''
                };
                 // If you want to clear filters when collapsing:
                this.filters = {
                     patientName: '',
                     hospitalId: '',
                     admissionDateStart: '',
                     admissionDateEnd: '',
                     dischargeDateStart: '',
                     dischargeDateEnd: '',
                     fileType: ''
                };
            }
        },
        async search(page = 1) {
            console.log('Search triggered with query:', this.query, 'filters:', this.filters, 'page:', page);
            this.current_page = page;
            this.error = '';
            const params = {
                query: this.query,
                patient_name: this.filters.patientName,
                hospital_id: this.filters.hospitalId,
                admission_date_start: this.filters.admissionDateStart,
                admission_date_end: this.filters.admissionDateEnd,
                discharge_date_start: this.filters.dischargeDateStart,
                discharge_date_end: this.filters.dischargeDateEnd,
                file_type: this.filters.fileType,
                page: this.current_page,
                size: this.size
            };
            console.log('Sending API request with params:', params);
            try {
                const response = await axios.get('/api/search', { params });
                console.log('Search results:', response.data.results);
                this.results = response.data.results;
                this.total = response.data.total;
                this.total_pages = response.data.total_pages;
                this.pdfUrl = null;
                this.pdfDocument = null;
                this.totalPdfPages = 1;
                this.pdfPage = 1;
            } catch (error) {
                this.error = '搜索失败，请重试';
                console.error('Search failed:', error);
            }
        },
        prevPage() {
            if (this.current_page > 1) {
                this.search(this.current_page - 1);
            }
        },
        nextPage() {
            if (this.current_page < this.total_pages) {
                this.search(this.current_page + 1);
            }
        },
        goToPage(page) {
            if (page >= 1 && page <= this.total_pages) {
                this.search(page);
            }
        },
        async loadPdf(result) {
            console.log('loadPdf called with result:', result);
            this.error = '';
            if (!result || !result.filename) {
                this.error = '无效的结果或缺少文件名';
                console.error('Invalid result or missing filename');
                return;
            }
            const newPdfUrl = `/api/pdf?filename=${encodeURIComponent(result.filename)}`;
            console.log('New pdfUrl:', newPdfUrl, 'Current pdfUrl:', this.pdfUrl);
            if (this.pdfUrl !== newPdfUrl) {
                this.pdfDocument = null;
                this.totalPdfPages = 1;
            }
            this.pdfUrl = newPdfUrl;
            this.pdfPage = result.page;
            console.log('pdfUrl set to:', this.pdfUrl, 'pdfPage:', this.pdfPage);
            await this.renderPdf();
        },
        async renderPdf() {
            console.log('renderPdf called with pdfUrl:', this.pdfUrl, 'page:', this.pdfPage);
            this.error = ''; // 清除之前的错误信息

            if (!this.pdfUrl) {
                console.log('No pdfUrl, skipping render');
                return;
            }
            if (typeof pdfjsLib === 'undefined') {
                this.error = 'PDF.js 未加载，请刷新页面';
                console.error('pdfjsLib is not defined');
                return;
            }
            try {
                 if (!this.pdfDocument || (this.pdfDocument.loadingTask && this.pdfDocument.loadingTask.url !== this.pdfUrl)) {
                    console.log('Loading new PDF document:', this.pdfUrl);
                    // 如果是新的 PDF URL，取消之前的加载任务（如果有）并加载新的
                     if (this.pdfDocument && this.pdfDocument.loadingTask && !this.pdfDocument.loadingTask.destroyed) { // Check if destroy method exists and not already destroyed
                        try {
                            if (typeof this.pdfDocument.loadingTask.destroy === 'function') {
                                 this.pdfDocument.loadingTask.destroy();
                            } else {
                                 console.warn("Previous loading task has no destroy method or already destroyed.");
                            }
                        } catch (e) { console.warn("Failed to destroy previous loading task:", e); }
                     }
                    const loadingTask = pdfjsLib.getDocument({
                        url: this.pdfUrl,
                        cMapUrl: '/static/pdfjs/web/cmaps/',
                        cMapPacked: true,
                        disableWorker: true // Enable worker for better performance
                    });
                    this.pdfDocument = await loadingTask.promise;
                    this.totalPdfPages = this.pdfDocument.numPages;
                    this.pdfcanvas  = document.getElementById('pdf-canvas');
                    this.pdfcanvasContext= this.pdfcanvas.getContext('2d');
                    //this.pdfPage = 1; 
                }  
                if (this.pdfPage < 1 || this.pdfPage > this.totalPdfPages) {
                     // Handle case where pdfDocument is the same, but the page number is out of bounds
                    this.error = `无效的页面编号: ${this.pdfPage}. 总页数: ${this.totalPdfPages}`;
                    console.error('Invalid page number for existing document:', this.pdfPage);
                     return; // Stop rendering if page is invalid
                }
                this.renderPage(this.pdfPage); // Render the current page
            } catch (error) {
                this.error = '加载 PDF 失败，请重试';
                console.error('renderPdf failed:', error);
                // 如果加载失败，清除文档对象，以便下次尝试重新加载
                if (this.pdfDocument && typeof this.pdfDocument.destroy === 'function') {
                    try { this.pdfDocument.destroy(); } catch (e) { console.warn("Failed to destroy document on render error:", e); }
                }
                this.pdfDocument = null;
                this.pdfUrl = null; // Clear PDF URL on catastrophic render failure
                this.totalPdfPages = 1; // Reset page count
                this.pdfPage = 1; // Reset page number
            }
        },
        // Render a specific page
        async renderPage(pageNo) {
            if (!this.pdfDocument) {
                console.error('PDF document is not loaded');
                return;
            }

            try {
                const page = await this.pdfDocument.getPage(pageNo);
                const viewport = this.getViewport(page)
                await this.renderCanvas(page, viewport);
                await this.renderTextLayer(page, viewport);
                this.renderingInProgress = false;
            } catch (error) {
                console.error('Error rendering page:', error);
                this.renderingInProgress = false;
            }
        },

        getViewport(page) {
            const viewport = page.getViewport({ scale: 1 });
            const container = document.querySelector('.pdf-viewer');
            const containerWidth = container ? container.clientWidth - 20 : 800;
            const scale = Math.min(containerWidth / viewport.width, 1.5); // 最大缩放1.5倍原始尺寸
            console.log('baseScale:', scale);
            return page.getViewport({ scale: scale });
        },
        // Render page to canvas
        async renderCanvas(page, viewport) {
            console.log('renderCanvas 的原始viewport', viewport);
            //高清显示屏的情况下 特殊处理
            const dpr = window.devicePixelRatio || 1; // 获取设备像素比，默认为 1
            if (dpr !== 1) {  
                this.pdfcanvas.style.width = `${viewport.width}px`;
                this.pdfcanvas.style.height = `${viewport.height -10}px`; // 减去10px,不留空隙
            }
            //高清显示屏的情况下,要把显示容器也要相应的放大
            this.pdfcanvas.height = viewport.height*dpr;
            this.pdfcanvas.width = viewport.width*dpr;
            this.pdfcanvasContext.scale(dpr, dpr);
            await page.render({
                canvasContext: this.pdfcanvasContext,
                viewport: viewport,
            }).promise;
            //恢复context
            this.pdfcanvasContext.setTransform(1, 0, 0, 1, 0, 0);
        },

        // Render text layer with highlights
        async renderTextLayer(page, viewport) {
            const textContent = await page.getTextContent();
            const textLayerDiv = document.getElementById('text-layer');
            textLayerDiv.innerHTML = '';
            textLayerDiv.style.setProperty('--scale-factor', viewport.scale);
            textLayerDiv.style.left = this.pdfcanvas.style.left;
            textLayerDiv.style.top = this.pdfcanvas.style.top;
            await pdfjsLib.renderTextLayer({
                textContentSource: textContent,
                container: textLayerDiv,
                viewport,
                textDivs: [],
            });

            if (this.query) { 
                // === 新增：处理查询字符串，替换特殊字符为空格 ===
                let processedQuery = this.query || '';
                processedQuery = processedQuery.replace(/[&|"'\\]/g, ' ').trim();
                const keywords = processedQuery.split(/\s+/).filter(k => k.length > 0);

                if (keywords.length > 0) {
                    textLayerDiv.querySelectorAll('span').forEach(div => {
                        let innerHTML = div.textContent;
                        keywords.forEach(keyword => {
                            // 转义关键词以防止正则表达式错误
                            const escapedKeyword = keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                            const regex = new RegExp(`(${escapedKeyword})`, 'gi');
                            innerHTML = innerHTML.replace(regex, '<span class="highlight">$1</span>');
                        });
                        if (innerHTML !== div.textContent) {
                            div.innerHTML = innerHTML;
                        }
                    });
                }
            }
        },

        prevPdfPage() {
            if (this.pdfPage > 1) {
                this.pdfPage--;
                this.renderPage(this.pdfPage);
            }
        },
        nextPdfPage() {
            if (this.pdfPage < this.totalPdfPages) {
                this.pdfPage++;
                this.renderPage(this.pdfPage);
            }
        },

        // New WebDAV methods
        async openSettingsModal() {
            await this.loadWebdavSettings();
            this.showSettingsModal = true;
        },
        closeSettingsModal() {
            this.showSettingsModal = false;
            // Optional: Reset form fields if needed, or keep last saved values
        },
        saveSettings() {
            // Basic validation (optional)
            if (this.webdavSettings.enabled) {
                if (!this.webdavSettings.ip || !this.webdavSettings.user || !this.webdavSettings.password) {
                    alert('启用 WebDAV 时，IP地址、用户名和密码不能为空。');
                    return;
                }
            }

            const settingsPayload = {
                ip: this.webdavSettings.ip,
                port: this.webdavSettings.port,
                user: this.webdavSettings.user,
                password: this.webdavSettings.password,
                directory: this.webdavSettings.directory,
                enabled: this.webdavSettings.enabled
            };

            console.log('Saving WebDAV settings:', settingsPayload);

            // Replace '/api/save_webdav_settings' with your actual backend endpoint
            axios.post('/api/save_webdav_settings', settingsPayload)
                .then(response => {
                    console.log('Settings saved successfully:', response.data);
                    alert('WebDAV 设置保存成功!');
                    this.closeSettingsModal();
                    // Optional: Handle successful save response (e.g., update local state based on backend confirmation)
                })
                .catch(error => {
                    console.error('Failed to save settings:', error);
                     let errorMessage = '保存 WebDAV 设置失败';
                    if (error.response && error.response.data && error.response.data.detail) {
                         errorMessage += ': ' + error.response.data.detail; // Assuming backend sends error details
                    } else if (error.message) {
                        errorMessage += ': ' + error.message;
                    }
                    alert(errorMessage);
                });
        },
         // Optional method to load settings on page load (call this in mounted if needed)
         async loadWebdavSettings() {
            try {
                 const response = await axios.get('/api/get_webdav_settings'); // Replace with your actual endpoint
                 console.log('Loaded existing WebDAV settings:', response.data);
                 this.webdavSettings = response.data; // Assuming backend returns an object matching webdavSettings structure
            } catch (error) {
                 console.error('Failed to load initial WebDAV settings:', error);
            }
         }
    }
});