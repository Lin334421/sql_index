-- 各公司在特定目录模块分类中的贡献占比

select 'google'               as company,
       contribution_type     as category,
       sum(total_insertions) as category_insertions,
       sum(total_deletions)  as category_deletions,
       sum(total_lines)      as category_lines
from (select *
from (select file_name,
             contribution_type,
             sum(insertions) as total_insertions,
             sum(deletions)  as total_deletions,
             sum(lines)      as total_lines
      from (select `files.file_name`  as file_name,
                   `files.insertions` as insertions,
                   `files.deletions`  as deletions,
                   `files.lines`      as lines,
                   multiIf(file_name like '%test%', '测试',
                           file_name like '%examples%', '样例',
                           file_name like '%benchmarks%', 'benchmarks',
                           file_name like 'c10/%' and file_name not like '%test%',
                           'c10',
                           (file_name like 'torch/csrc/generic/%' or file_name like 'torch/csrc/Module.cpp' or
                            file_name like 'THCTensorMath.cu' or file_name like 'generic/THTensorMath.c' or
                            file_name like 'generic/THTensorLapack.c' or file_name like 'THCTensorRandom.cu' or
                            file_name like 'torch/Tensor.py' or
                            file_name like 'torch/lib/THD/master_worker/master/generic/THDTensor.cpp' or
                            file_name like 'THCTensor.c' or file_name like 'torch/tensor.py' or
                            file_name like 'THCTensorMath.h') and
                           file_name not like '%test%', '张量操作通用方法',
                           (file_name like 'torch/csrc/utils.cpp' or file_name like 'tools/%' or
                            file_name like 'torch/utils/%' or file_name like 'torch/profiler/profiler.py') and
                           file_name not like '%test%', 'tools 工具包',
                           (file_name like 'setup.py' or file_name like 'cmake/%' or file_name like '.circleci/%' or
                            file_name like '.ci/%' or file_name like '.azure_pipelines/%' or
                            file_name like 'CMakeLists.txt' or file_name like 'docker%' or
                            file_name like 'torch/CMakeLists.txt' or file_name like '.jenkins%') and
                           file_name not like '%test%', '项目构建集成安装',
                           (file_name like 'torch/csrc/distributed/%' or file_name like 'torch/lib/THD/%' or
                            file_name like 'torch/distributed/%' or file_name like 'torch/lib/c10d%' or
                            file_name like 'torch/csrc/api/src/data/samplers/distributed.cpp') and
                           file_name not like '%test%', '分布式计算',
                           (file_name like '%/onnx/%') and
                           file_name not like '%test%', 'onnx',
                           (file_name like 'torch/autograd/%' or file_name like 'torch/csrc/autograd/%') and
                           file_name not like '%test%', '自动微分autograd算法',
                           (file_name like 'torch/legacy/nn%' or
                            file_name like 'torch/nn/%' or file_name like 'generic/THNN.h' or
                            file_name like 'torch/lib/THNN/%') and
                           file_name not like '%test%', '深度学习神经网络相关',
                           file_name like 'torch/ao/quantization%' and file_name not like '%test%',
                           'torch_quantization优化模型性能',
                           (file_name like '%cuda%' or
                            file_name like 'THAllocator.c' or file_name like 'torch/lib/THCUNN%' or
                            file_name like '%/cudnn/%' or file_name like '%/cunn/%' or file_name like '%.cu' or
                            file_name like 'THCUNN.h') and file_name not like '%test%',
                           'cuda',
                           (file_name like 'torch/_inductor/%' or file_name like 'torch/csrc/inductor%' or
                            file_name like 'torch/csrc/dynamo/guards.cpp') and
                           file_name not like '%test%', 'inductor编译器',
                           file_name like 'caffe2%' and file_name not like '%test%', 'caffe2优化',
                           (file_name like 'torch/csrc/jit/%' or
                            file_name like 'torch/csrc/lazy/core/lazy_graph_executor.cpp' or
                            file_name like 'torch/jit/%' or file_name like 'torch/jit.py') and
                           file_name not like '%test%', 'jit编译',
                           file_name like 'aten/src/%' and file_name not like '%test%', 'aten',
                           (file_name like 'docs%' or file_name like 'doc/%' or file_name like 'README.md' or
                            file_name like '%.md'), '文档',
                           file_name like '%xpu/%' and file_name not like '%test%', 'xpu',
                           file_name like '%torch/optim/%%' and file_name not like '%test%', '优化算法',
                           '')        as contribution_type
            from (select a.*, if(b.author__login = '', a.author_name, b.author__login) as user_id, company
                  from (select search_key__owner,
                               search_key__repo,
                               hexsha,
                               message,
                               author_name,
                               author_email,
                               `files.file_name`,
                               `files.insertions`,
                               `files.deletions`,
                               `files.lines`,
                               total__lines,
                               total__insertions,
                               total__deletions,
                               toYYYYMM(authored_date) as month
                        from gits
                        where search_key__owner = 'pytorch'
                           --               and toYYYYMMDD(authored_date) <= 20210616
--               and toYYYYMMDD(authored_date) > 20200422
                           ) as a global
                           join (
                      select search_key__owner, search_key__repo, sha, author__login, company
                      from commit_company
                      where company = 'google'
                        and (search_key__owner = 'pytorch' and search_key__repo global in ('pytorch',
-- 'glow',
-- 'tutorials',
-- 'ort',
-- 'FBGEMM'
                          ))
                      group by search_key__owner, search_key__repo, sha, author__login, company
                      ) as b
                                on a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo
                                    and a.hexsha = b.sha
                  order by search_key__repo) array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`)
      group by file_name, contribution_type
      order by total_lines desc)
where contribution_type != ''
  and file_name not like '.github%')
group by contribution_type
order by category_lines desc

union all

select 'intel'               as company,
       contribution_type     as category,
       sum(total_insertions) as category_insertions,
       sum(total_deletions)  as category_deletions,
       sum(total_lines)      as category_lines
from (select *
from (select file_name,
             contribution_type,
             sum(insertions) as total_insertions,
             sum(deletions)  as total_deletions,
             sum(lines)      as total_lines
      from (select `files.file_name`  as file_name,
                   `files.insertions` as insertions,
                   `files.deletions`  as deletions,
                   `files.lines`      as lines,
                   multiIf(file_name like '%test%', '测试',
                           file_name like '%examples%', '样例',
                           file_name like '%benchmarks%', 'benchmarks',
                           file_name like 'c10/%' and file_name not like '%test%',
                           'c10',
                           (file_name like 'torch/csrc/generic/%' or file_name like 'torch/csrc/Module.cpp' or
                            file_name like 'THCTensorMath.cu' or file_name like 'generic/THTensorMath.c' or
                            file_name like 'generic/THTensorLapack.c' or file_name like 'THCTensorRandom.cu' or
                            file_name like 'torch/Tensor.py' or
                            file_name like 'torch/lib/THD/master_worker/master/generic/THDTensor.cpp' or
                            file_name like 'THCTensor.c' or file_name like 'torch/tensor.py' or
                            file_name like 'THCTensorMath.h') and
                           file_name not like '%test%', '张量操作通用方法',
                           (file_name like 'torch/csrc/utils.cpp' or file_name like 'tools/%' or
                            file_name like 'torch/utils/%' or file_name like 'torch/profiler/profiler.py') and
                           file_name not like '%test%', 'tools 工具包',
                           (file_name like 'setup.py' or file_name like 'cmake/%' or file_name like '.circleci/%' or
                            file_name like '.ci/%' or file_name like '.azure_pipelines/%' or
                            file_name like 'CMakeLists.txt' or file_name like 'docker%' or
                            file_name like 'torch/CMakeLists.txt' or file_name like '.jenkins%') and
                           file_name not like '%test%', '项目构建集成安装',
                           (file_name like 'torch/csrc/distributed/%' or file_name like 'torch/lib/THD/%' or
                            file_name like 'torch/distributed/%' or file_name like 'torch/lib/c10d%' or
                            file_name like 'torch/csrc/api/src/data/samplers/distributed.cpp') and
                           file_name not like '%test%', '分布式计算',
                           (file_name like '%/onnx/%') and
                           file_name not like '%test%', 'onnx',
                           (file_name like 'torch/autograd/%' or file_name like 'torch/csrc/autograd/%') and
                           file_name not like '%test%', '自动微分autograd算法',
                           (file_name like 'torch/legacy/nn%' or
                            file_name like 'torch/nn/%' or file_name like 'generic/THNN.h' or
                            file_name like 'torch/lib/THNN/%') and
                           file_name not like '%test%', '深度学习神经网络相关',
                           file_name like 'torch/ao/quantization%' and file_name not like '%test%',
                           'torch_quantization优化模型性能',
                           (file_name like '%cuda%' or
                            file_name like 'THAllocator.c' or file_name like 'torch/lib/THCUNN%' or
                            file_name like '%/cudnn/%' or file_name like '%/cunn/%' or file_name like '%.cu' or
                            file_name like 'THCUNN.h') and file_name not like '%test%',
                           'cuda',
                           (file_name like 'torch/_inductor/%' or file_name like 'torch/csrc/inductor%' or
                            file_name like 'torch/csrc/dynamo/guards.cpp') and
                           file_name not like '%test%', 'inductor编译器',
                           file_name like 'caffe2%' and file_name not like '%test%', 'caffe2优化',
                           (file_name like 'torch/csrc/jit/%' or
                            file_name like 'torch/csrc/lazy/core/lazy_graph_executor.cpp' or
                            file_name like 'torch/jit/%' or file_name like 'torch/jit.py') and
                           file_name not like '%test%', 'jit编译',
                           file_name like 'aten/src/%' and file_name not like '%test%', 'aten',
                           (file_name like 'docs%' or file_name like 'doc/%' or file_name like 'README.md' or
                            file_name like '%.md'), '文档',
                           file_name like '%xpu/%' and file_name not like '%test%', 'xpu',
                           file_name like '%torch/optim/%%' and file_name not like '%test%', '优化算法',
                           '')        as contribution_type
            from (select a.*, if(b.author__login = '', a.author_name, b.author__login) as user_id, company
                  from (select search_key__owner,
                               search_key__repo,
                               hexsha,
                               message,
                               author_name,
                               author_email,
                               `files.file_name`,
                               `files.insertions`,
                               `files.deletions`,
                               `files.lines`,
                               total__lines,
                               total__insertions,
                               total__deletions,
                               toYYYYMM(authored_date) as month
                        from gits
                        where search_key__owner = 'pytorch'
                           --               and toYYYYMMDD(authored_date) <= 20210616
--               and toYYYYMMDD(authored_date) > 20200422
                           ) as a global
                           join (
                      select search_key__owner, search_key__repo, sha, author__login, company
                      from commit_company
                      where company = 'intel'
                        and (search_key__owner = 'pytorch' and search_key__repo global in ('pytorch',
-- 'glow',
-- 'tutorials',
-- 'ort',
-- 'FBGEMM'
                          ))
                      group by search_key__owner, search_key__repo, sha, author__login, company
                      ) as b
                                on a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo
                                    and a.hexsha = b.sha
                  order by search_key__repo) array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`)
      group by file_name, contribution_type
      order by total_lines desc)
where contribution_type != ''
  and file_name not like '.github%')
group by contribution_type
order by category_lines desc



union all

select 'microsoft'               as company,
       contribution_type     as category,
       sum(total_insertions) as category_insertions,
       sum(total_deletions)  as category_deletions,
       sum(total_lines)      as category_lines
from (select *
from (select file_name,
             contribution_type,
             sum(insertions) as total_insertions,
             sum(deletions)  as total_deletions,
             sum(lines)      as total_lines
      from (select `files.file_name`  as file_name,
                   `files.insertions` as insertions,
                   `files.deletions`  as deletions,
                   `files.lines`      as lines,
                   multiIf(file_name like '%test%', '测试',
                           file_name like '%examples%', '样例',
                           file_name like '%benchmarks%', 'benchmarks',
                           file_name like 'c10/%' and file_name not like '%test%',
                           'c10',
                           (file_name like 'torch/csrc/generic/%' or file_name like 'torch/csrc/Module.cpp' or
                            file_name like 'THCTensorMath.cu' or file_name like 'generic/THTensorMath.c' or
                            file_name like 'generic/THTensorLapack.c' or file_name like 'THCTensorRandom.cu' or
                            file_name like 'torch/Tensor.py' or
                            file_name like 'torch/lib/THD/master_worker/master/generic/THDTensor.cpp' or
                            file_name like 'THCTensor.c' or file_name like 'torch/tensor.py' or
                            file_name like 'THCTensorMath.h') and
                           file_name not like '%test%', '张量操作通用方法',
                           (file_name like 'torch/csrc/utils.cpp' or file_name like 'tools/%' or
                            file_name like 'torch/utils/%' or file_name like 'torch/profiler/profiler.py') and
                           file_name not like '%test%', 'tools 工具包',
                           (file_name like 'setup.py' or file_name like 'cmake/%' or file_name like '.circleci/%' or
                            file_name like '.ci/%' or file_name like '.azure_pipelines/%' or
                            file_name like 'CMakeLists.txt' or file_name like 'docker%' or
                            file_name like 'torch/CMakeLists.txt' or file_name like '.jenkins%') and
                           file_name not like '%test%', '项目构建集成安装',
                           (file_name like 'torch/csrc/distributed/%' or file_name like 'torch/lib/THD/%' or
                            file_name like 'torch/distributed/%' or file_name like 'torch/lib/c10d%' or
                            file_name like 'torch/csrc/api/src/data/samplers/distributed.cpp') and
                           file_name not like '%test%', '分布式计算',
                           (file_name like '%/onnx/%') and
                           file_name not like '%test%', 'onnx',
                           (file_name like 'torch/autograd/%' or file_name like 'torch/csrc/autograd/%') and
                           file_name not like '%test%', '自动微分autograd算法',
                           (file_name like 'torch/legacy/nn%' or
                            file_name like 'torch/nn/%' or file_name like 'generic/THNN.h' or
                            file_name like 'torch/lib/THNN/%') and
                           file_name not like '%test%', '深度学习神经网络相关',
                           file_name like 'torch/ao/quantization%' and file_name not like '%test%',
                           'torch_quantization优化模型性能',
                           (file_name like '%cuda%' or
                            file_name like 'THAllocator.c' or file_name like 'torch/lib/THCUNN%' or
                            file_name like '%/cudnn/%' or file_name like '%/cunn/%' or file_name like '%.cu' or
                            file_name like 'THCUNN.h') and file_name not like '%test%',
                           'cuda',
                           (file_name like 'torch/_inductor/%' or file_name like 'torch/csrc/inductor%' or
                            file_name like 'torch/csrc/dynamo/guards.cpp') and
                           file_name not like '%test%', 'inductor编译器',
                           file_name like 'caffe2%' and file_name not like '%test%', 'caffe2优化',
                           (file_name like 'torch/csrc/jit/%' or
                            file_name like 'torch/csrc/lazy/core/lazy_graph_executor.cpp' or
                            file_name like 'torch/jit/%' or file_name like 'torch/jit.py') and
                           file_name not like '%test%', 'jit编译',
                           file_name like 'aten/src/%' and file_name not like '%test%', 'aten',
                           (file_name like 'docs%' or file_name like 'doc/%' or file_name like 'README.md' or
                            file_name like '%.md'), '文档',
                           file_name like '%xpu/%' and file_name not like '%test%', 'xpu',
                           file_name like '%torch/optim/%%' and file_name not like '%test%', '优化算法',
                           '')        as contribution_type
            from (select a.*, if(b.author__login = '', a.author_name, b.author__login) as user_id, company
                  from (select search_key__owner,
                               search_key__repo,
                               hexsha,
                               message,
                               author_name,
                               author_email,
                               `files.file_name`,
                               `files.insertions`,
                               `files.deletions`,
                               `files.lines`,
                               total__lines,
                               total__insertions,
                               total__deletions,
                               toYYYYMM(authored_date) as month
                        from gits
                        where search_key__owner = 'pytorch'
                           --               and toYYYYMMDD(authored_date) <= 20210616
--               and toYYYYMMDD(authored_date) > 20200422
                           ) as a global
                           join (
                      select search_key__owner, search_key__repo, sha, author__login, company
                      from commit_company
                      where company = 'microsoft'
                        and (search_key__owner = 'pytorch' and search_key__repo global in ('pytorch',
-- 'glow',
-- 'tutorials',
-- 'ort',
-- 'FBGEMM'
                          ))
                      group by search_key__owner, search_key__repo, sha, author__login, company
                      ) as b
                                on a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo
                                    and a.hexsha = b.sha
                  order by search_key__repo) array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`)
      group by file_name, contribution_type
      order by total_lines desc)
where contribution_type != ''
  and file_name not like '.github%')
group by contribution_type
order by category_lines desc



union all

select 'amd'               as company,
       contribution_type     as category,
       sum(total_insertions) as category_insertions,
       sum(total_deletions)  as category_deletions,
       sum(total_lines)      as category_lines
from (select *
from (select file_name,
             contribution_type,
             sum(insertions) as total_insertions,
             sum(deletions)  as total_deletions,
             sum(lines)      as total_lines
      from (select `files.file_name`  as file_name,
                   `files.insertions` as insertions,
                   `files.deletions`  as deletions,
                   `files.lines`      as lines,
                   multiIf(file_name like '%test%', '测试',
                           file_name like '%examples%', '样例',
                           file_name like '%benchmarks%', 'benchmarks',
                           file_name like 'c10/%' and file_name not like '%test%',
                           'c10',
                           (file_name like 'torch/csrc/generic/%' or file_name like 'torch/csrc/Module.cpp' or
                            file_name like 'THCTensorMath.cu' or file_name like 'generic/THTensorMath.c' or
                            file_name like 'generic/THTensorLapack.c' or file_name like 'THCTensorRandom.cu' or
                            file_name like 'torch/Tensor.py' or
                            file_name like 'torch/lib/THD/master_worker/master/generic/THDTensor.cpp' or
                            file_name like 'THCTensor.c' or file_name like 'torch/tensor.py' or
                            file_name like 'THCTensorMath.h') and
                           file_name not like '%test%', '张量操作通用方法',
                           (file_name like 'torch/csrc/utils.cpp' or file_name like 'tools/%' or
                            file_name like 'torch/utils/%' or file_name like 'torch/profiler/profiler.py') and
                           file_name not like '%test%', 'tools 工具包',
                           (file_name like 'setup.py' or file_name like 'cmake/%' or file_name like '.circleci/%' or
                            file_name like '.ci/%' or file_name like '.azure_pipelines/%' or
                            file_name like 'CMakeLists.txt' or file_name like 'docker%' or
                            file_name like 'torch/CMakeLists.txt' or file_name like '.jenkins%') and
                           file_name not like '%test%', '项目构建集成安装',
                           (file_name like 'torch/csrc/distributed/%' or file_name like 'torch/lib/THD/%' or
                            file_name like 'torch/distributed/%' or file_name like 'torch/lib/c10d%' or
                            file_name like 'torch/csrc/api/src/data/samplers/distributed.cpp') and
                           file_name not like '%test%', '分布式计算',
                           (file_name like '%/onnx/%') and
                           file_name not like '%test%', 'onnx',
                           (file_name like 'torch/autograd/%' or file_name like 'torch/csrc/autograd/%') and
                           file_name not like '%test%', '自动微分autograd算法',
                           (file_name like 'torch/legacy/nn%' or
                            file_name like 'torch/nn/%' or file_name like 'generic/THNN.h' or
                            file_name like 'torch/lib/THNN/%') and
                           file_name not like '%test%', '深度学习神经网络相关',
                           file_name like 'torch/ao/quantization%' and file_name not like '%test%',
                           'torch_quantization优化模型性能',
                           (file_name like '%cuda%' or
                            file_name like 'THAllocator.c' or file_name like 'torch/lib/THCUNN%' or
                            file_name like '%/cudnn/%' or file_name like '%/cunn/%' or file_name like '%.cu' or
                            file_name like 'THCUNN.h') and file_name not like '%test%',
                           'cuda',
                           (file_name like 'torch/_inductor/%' or file_name like 'torch/csrc/inductor%' or
                            file_name like 'torch/csrc/dynamo/guards.cpp') and
                           file_name not like '%test%', 'inductor编译器',
                           file_name like 'caffe2%' and file_name not like '%test%', 'caffe2优化',
                           (file_name like 'torch/csrc/jit/%' or
                            file_name like 'torch/csrc/lazy/core/lazy_graph_executor.cpp' or
                            file_name like 'torch/jit/%' or file_name like 'torch/jit.py') and
                           file_name not like '%test%', 'jit编译',
                           file_name like 'aten/src/%' and file_name not like '%test%', 'aten',
                           (file_name like 'docs%' or file_name like 'doc/%' or file_name like 'README.md' or
                            file_name like '%.md'), '文档',
                           file_name like '%xpu/%' and file_name not like '%test%', 'xpu',
                           file_name like '%torch/optim/%%' and file_name not like '%test%', '优化算法',
                           '')        as contribution_type
            from (select a.*, if(b.author__login = '', a.author_name, b.author__login) as user_id, company
                  from (select search_key__owner,
                               search_key__repo,
                               hexsha,
                               message,
                               author_name,
                               author_email,
                               `files.file_name`,
                               `files.insertions`,
                               `files.deletions`,
                               `files.lines`,
                               total__lines,
                               total__insertions,
                               total__deletions,
                               toYYYYMM(authored_date) as month
                        from gits
                        where search_key__owner = 'pytorch'
                           --               and toYYYYMMDD(authored_date) <= 20210616
--               and toYYYYMMDD(authored_date) > 20200422
                           ) as a global
                           join (
                      select search_key__owner, search_key__repo, sha, author__login, company
                      from commit_company
                      where company = 'amd'
                        and (search_key__owner = 'pytorch' and search_key__repo global in ('pytorch',
-- 'glow',
-- 'tutorials',
-- 'ort',
-- 'FBGEMM'
                          ))
                      group by search_key__owner, search_key__repo, sha, author__login, company
                      ) as b
                                on a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo
                                    and a.hexsha = b.sha
                  order by search_key__repo) array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`)
      group by file_name, contribution_type
      order by total_lines desc)
where contribution_type != ''
  and file_name not like '.github%')
group by contribution_type
order by category_lines desc



union all

select 'huawei'               as company,
       contribution_type     as category,
       sum(total_insertions) as category_insertions,
       sum(total_deletions)  as category_deletions,
       sum(total_lines)      as category_lines
from (select *
from (select file_name,
             contribution_type,
             sum(insertions) as total_insertions,
             sum(deletions)  as total_deletions,
             sum(lines)      as total_lines
      from (select `files.file_name`  as file_name,
                   `files.insertions` as insertions,
                   `files.deletions`  as deletions,
                   `files.lines`      as lines,
                   multiIf(file_name like '%test%', '测试',
                           file_name like '%examples%', '样例',
                           file_name like '%benchmarks%', 'benchmarks',
                           file_name like 'c10/%' and file_name not like '%test%',
                           'c10',
                           (file_name like 'torch/csrc/generic/%' or file_name like 'torch/csrc/Module.cpp' or
                            file_name like 'THCTensorMath.cu' or file_name like 'generic/THTensorMath.c' or
                            file_name like 'generic/THTensorLapack.c' or file_name like 'THCTensorRandom.cu' or
                            file_name like 'torch/Tensor.py' or
                            file_name like 'torch/lib/THD/master_worker/master/generic/THDTensor.cpp' or
                            file_name like 'THCTensor.c' or file_name like 'torch/tensor.py' or
                            file_name like 'THCTensorMath.h') and
                           file_name not like '%test%', '张量操作通用方法',
                           (file_name like 'torch/csrc/utils.cpp' or file_name like 'tools/%' or
                            file_name like 'torch/utils/%' or file_name like 'torch/profiler/profiler.py') and
                           file_name not like '%test%', 'tools 工具包',
                           (file_name like 'setup.py' or file_name like 'cmake/%' or file_name like '.circleci/%' or
                            file_name like '.ci/%' or file_name like '.azure_pipelines/%' or
                            file_name like 'CMakeLists.txt' or file_name like 'docker%' or
                            file_name like 'torch/CMakeLists.txt' or file_name like '.jenkins%') and
                           file_name not like '%test%', '项目构建集成安装',
                           (file_name like 'torch/csrc/distributed/%' or file_name like 'torch/lib/THD/%' or
                            file_name like 'torch/distributed/%' or file_name like 'torch/lib/c10d%' or
                            file_name like 'torch/csrc/api/src/data/samplers/distributed.cpp') and
                           file_name not like '%test%', '分布式计算',
                           (file_name like '%/onnx/%') and
                           file_name not like '%test%', 'onnx',
                           (file_name like 'torch/autograd/%' or file_name like 'torch/csrc/autograd/%') and
                           file_name not like '%test%', '自动微分autograd算法',
                           (file_name like 'torch/legacy/nn%' or
                            file_name like 'torch/nn/%' or file_name like 'generic/THNN.h' or
                            file_name like 'torch/lib/THNN/%') and
                           file_name not like '%test%', '深度学习神经网络相关',
                           file_name like 'torch/ao/quantization%' and file_name not like '%test%',
                           'torch_quantization优化模型性能',
                           (file_name like '%cuda%' or
                            file_name like 'THAllocator.c' or file_name like 'torch/lib/THCUNN%' or
                            file_name like '%/cudnn/%' or file_name like '%/cunn/%' or file_name like '%.cu' or
                            file_name like 'THCUNN.h') and file_name not like '%test%',
                           'cuda',
                           (file_name like 'torch/_inductor/%' or file_name like 'torch/csrc/inductor%' or
                            file_name like 'torch/csrc/dynamo/guards.cpp') and
                           file_name not like '%test%', 'inductor编译器',
                           file_name like 'caffe2%' and file_name not like '%test%', 'caffe2优化',
                           (file_name like 'torch/csrc/jit/%' or
                            file_name like 'torch/csrc/lazy/core/lazy_graph_executor.cpp' or
                            file_name like 'torch/jit/%' or file_name like 'torch/jit.py') and
                           file_name not like '%test%', 'jit编译',
                           file_name like 'aten/src/%' and file_name not like '%test%', 'aten',
                           (file_name like 'docs%' or file_name like 'doc/%' or file_name like 'README.md' or
                            file_name like '%.md'), '文档',
                           file_name like '%xpu/%' and file_name not like '%test%', 'xpu',
                           file_name like '%torch/optim/%%' and file_name not like '%test%', '优化算法',
                           '')        as contribution_type
            from (select a.*, if(b.author__login = '', a.author_name, b.author__login) as user_id, company
                  from (select search_key__owner,
                               search_key__repo,
                               hexsha,
                               message,
                               author_name,
                               author_email,
                               `files.file_name`,
                               `files.insertions`,
                               `files.deletions`,
                               `files.lines`,
                               total__lines,
                               total__insertions,
                               total__deletions,
                               toYYYYMM(authored_date) as month
                        from gits
                        where search_key__owner = 'pytorch'
                           --               and toYYYYMMDD(authored_date) <= 20210616
--               and toYYYYMMDD(authored_date) > 20200422
                           ) as a global
                           join (
                      select search_key__owner, search_key__repo, sha, author__login, company
                      from commit_company
                      where company = 'huawei'
                        and (search_key__owner = 'pytorch' and search_key__repo global in ('pytorch',
-- 'glow',
-- 'tutorials',
-- 'ort',
-- 'FBGEMM'
                          ))
                      group by search_key__owner, search_key__repo, sha, author__login, company
                      ) as b
                                on a.search_key__owner = b.search_key__owner
                                    and a.search_key__repo = b.search_key__repo
                                    and a.hexsha = b.sha
                  order by search_key__repo) array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`)
      group by file_name, contribution_type
      order by total_lines desc)
where contribution_type != ''
  and file_name not like '.github%')
group by contribution_type
order by category_lines desc

;
[
    {
        "owner": "microsoft",
        "repo": "TypeScript"
    },
    {
        "owner": "microsoft",
        "repo": "vscode"
    }
]

select commit_company.search_key__owner,
       commit_company.search_key__repo,
       concat(search_key__owner, '__', search_key__repo) as owner_repo,
       if(company = '', '其他', company)                 as company,
       count()                                              commit_count
from commit_company
where (search_key__owner = 'microsoft' and search_key__repo = 'TypeScript')
group by commit_company.search_key__owner, commit_company.search_key__repo, company
order by commit_company.search_key__owner, commit_company.search_key__repo, commit_count desc


;


;

select search_key__repo, sum(commit_count)
from (select search_key__repo, company, count() as commit_count
      from (select search_key__repo, hexsha, if(company = '', email_company, company) as company
            from (select a.*, b.company as email_company
                  from (select a.*, b.company
                        from (select search_key__repo, hexsha, author_email
                              from gits
                              where search_key__owner = 'microsoft'
                                and search_key__repo = 'TypeScript'
                                and author_email not like '%[bot]%'
                                and length(parents) = 1) as a global
                                 left join (select sha, company, author__login
                                            from commit_company
                                            where search_key__owner = 'microsoft'
                                              and search_key__repo = 'TypeScript'
                                            group by sha, company, author__login) as b on a.hexsha = b.sha) as a global
                           left join (select * from company_email_map) as b
                                     on splitByChar('@', a.author_email)[2] = b.email_domain))
      group by search_key__repo, company) where company!='microsoft'
group by search_key__repo


;


select search_key__repo, sum(commit_count)
from (select commit_company.search_key__owner,
             commit_company.search_key__repo,
             concat(search_key__owner, '__', search_key__repo) as owner_repo,
             if(company = '', '其他', company)                 as company,
             count()                                              commit_count
      from commit_company
      where (search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo')
        and company != 'nvidia'
      group by commit_company.search_key__owner, commit_company.search_key__repo, company
      order by commit_company.search_key__owner, commit_company.search_key__repo, commit_count desc)
group by search_key__repo
;

select round((103533/(9617+103533))*100,2)   8.5  91.5
select round((5120/(1046+5120))*100,2)  16.96 83.04
select round((22919/(5553+22919))*100,2)  19.5 80.5


;

select search_key__owner,
                   search_key__repo,
                   JSONExtractString(timeline_raw, 'id')                               as id,
                   JSONExtractString(timeline_raw, 'state')                            as state,
                   JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'login') as login
            from github_issues_timeline
            where search_key__event = 'reviewed'
              and search_key__owner = 'microsoft'
              and search_key__repo = 'TypeScript'
--               and state = 'approved'
            group by search_key__owner, search_key__repo, id, state, login
;

select a.*, company
from (select search_key__owner, search_key__repo, login, count() as approved_count
      from (select search_key__owner,
                   search_key__repo,
                   JSONExtractString(timeline_raw, 'id')                               as id,
                   JSONExtractString(timeline_raw, 'state')                            as state,
                   JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'login') as login
            from github_issues_timeline
            where search_key__event = 'reviewed'
              and (search_key__owner = 'microsoft'
              and search_key__repo = 'TypeScript' or search_key__owner = 'microsoft'
                            and search_key__repo = 'vscode')
              and (state = 'approved' or state = 'changes_requested' or state = 'dismissed')
            group by search_key__owner, search_key__repo, id, state, login)
      group by search_key__owner, search_key__repo, login
      having approved_count > 10
      order by search_key__owner, search_key__repo, approved_count desc
         ) as a global
         left join (select author__login, company
                    from (select author__login, company
                          from commit_company
                          where (search_key__owner = 'microsoft'
                            and search_key__repo = 'TypeScript'
                                     or search_key__owner = 'microsoft'
                            and search_key__repo = 'vscode')
                            and author__login != ''
                            and commit_company.company != ''
                          group by author__login, company
                          union all
                          select login,
                                 if(final_company_inferred_from_company = 'facebook',
                                    'meta',
                                    final_company_inferred_from_company) as company
                          from github_profile
                          where github_profile.final_company_inferred_from_company != ''
                            and final_company_inferred_from_company != '无'
                          group by login, final_company_inferred_from_company)
                    group by author__login, company
                    order by author__login) as b on a.login = b.author__login


-- torch 目录


;
-- aten
-- c10
-- caffe2
-- functorch
-- torch
-- 指定目录2层
select search_key__owner,
       search_key__repo,
       if(company='','其他',company) as company,
       year,
       dir_level_1 as dir_hierarchy_1,
       dir_level_4 as dir_hierarchy_4,
       count(distinct hexsha) as commit_count,
       count(distinct lower(user_id)) as developer_count,
       sum(insertions)        as total_insertions,
       sum(deletions)         as total_deletions,
       sum(lines)             as total_lines
from (select splitByChar('/', file_name)                                        as dir_list
       ,
          if(length(dir_list) > 2, concat(dir_list[1], '/' ), '') as dir_level_1,
       if(length(dir_list) > 4, concat(dir_list[1], '/' ,dir_list[2], '/' ,dir_list[3], '/' ,dir_list[4]), '') as dir_level_4,*
from (select *, if(author__login = '', author_name, author__login) as user_id
      from (select search_key__owner,
                   search_key__repo,
                   hexsha,
                   author_name,
                   author_email,
                   parents,
                   file_name,
                   insertions,
                   deletions,
                   lines,
                   year,
                   b.company as company,
                   author__login
            from (select *
                  from (select a.*, b.company, b.author__login
                        from (select search_key__owner,
                                     search_key__repo,
                                     hexsha,
                                     author_name,
                                     author_email,
                                     parents,
                                     `files.file_name`     as file_name,
                                     `files.insertions`       insertions,
                                     `files.deletions`        deletions,
                                     `files.lines`            lines,
                                     toYear(authored_date) as year
                              from gits array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`
                              where search_key__owner = 'pytorch'
                                and search_key__repo = 'pytorch'
--                                 and (file_name like 'torch/%' or file_name like 'aten/%' or file_name like 'c10%' or file_name like 'caffe2/%' or file_name like 'functorch/%')
                                                              and file_name not like '%{%'

--                                 and file_name not like '%test%'
--                                 and file_name not like '%.md'
--                                 and file_name not like '%examples%'
                                and author_name global not in ('PyTorch MergeBot',
                                                                         'onnxbot',
                                                                         'PyTorch UpdateBot',
                                                                         'Facebook Community Bot',
                                                                         'CodemodService FBSourceClangFormatLinterBot',
                                                                         'dependabot[bot]',
                                                                         'CodemodService Bot',
                                                                         'CodemodService FBSourceGoogleJavaFormatLinterBot',
                                                                         'CodemodService FBSourceBuckFormatLinterBot',
                                                                         'CodemodService FBSourceBlackLinterBot',
                                                                         'pytorchbot',
                                                                         'Facebook Github Bot'
                                            )) as a global
                                 left join (select sha, company, author__login
                                            from commit_company
                                            where search_key__owner = 'pytorch'
                                              and search_key__repo = 'pytorch'
                                            group by sha, company, author__login) as b on a.hexsha = b.sha)
                  where company = '') as a global
                     left join (select * from company_email_map) as b
                               on splitByChar('@', a.author_email)[2] = b.email_domain

            union all
            select *
            from (select a.*, b.company, b.author__login
                  from (select search_key__owner,
                               search_key__repo,
                               hexsha,
                               author_name,
                               author_email,
                               parents,
                               `files.file_name`     as file_name,
                               `files.insertions`       insertions,
                               `files.deletions`        deletions,
                               `files.lines`            lines,
                               toYear(authored_date) as year
                        from gits array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`
                        where search_key__owner = 'pytorch'
                          and search_key__repo = 'pytorch'
--                           and (file_name like 'torch/%' or file_name like 'aten/%' or file_name like 'c10%' or file_name like 'caffe2/%' or file_name like 'functorch/%')
                            and file_name not like '%{%'
--                           and file_name not like '%test%'
--                           and file_name not like '%.md'
--                           and file_name not like '%examples%'
                          and author_name global not in ('PyTorch MergeBot',
                                                                         'onnxbot',
                                                                         'PyTorch UpdateBot',
                                                                         'Facebook Community Bot',
                                                                         'CodemodService FBSourceClangFormatLinterBot',
                                                                         'dependabot[bot]',
                                                                         'CodemodService Bot',
                                                                         'CodemodService FBSourceGoogleJavaFormatLinterBot',
                                                                         'CodemodService FBSourceBuckFormatLinterBot',
                                                                         'CodemodService FBSourceBlackLinterBot',
                                                                         'pytorchbot',
                                                                         'Facebook Github Bot'
                                            )) as a global
                           left join (select sha, company, author__login
                                      from commit_company
                                      where search_key__owner = 'pytorch'
                                        and search_key__repo = 'pytorch'
                                      group by sha, company, author__login) as b on a.hexsha = b.sha)
            where company != '')))
where
--     company global  in (
-- --                           'google',
-- -- 'meta',
-- -- 'twitter',
-- -- 'lightning.ai',
-- -- 'naver',
-- -- 'nvidia'
-- --     ,
-- -- 'microsoft',
-- -- 'ibm',
-- -- 'quansight',
-- -- 'fujitsu',
-- -- 'amd',
-- -- 'openai',
-- -- 'intel',
-- -- 'amazon',
-- -- 'huawei',
-- -- 'tencent',
-- -- 'apple',
-- -- 'octoml',
-- -- 'bytedance',
-- -- 'alibaba'
--
--                          ) and
    dir_level_4 !=''
group by search_key__owner, search_key__repo, company,year,dir_level_1,dir_level_4
order by year,dir_hierarchy_1,commit_count desc

;
select * from gits limit 1
;
select dir_level_3,
       sum(deletions)         as total_deletions,
       sum(insertion)         as total_insertion,
       count(distinct hexsha) as commit_count
from (select hexsha,
             author_email,
             `files.file_name`                                                                     as file_name,
             `files.deletions`                                                                     as deletions,
             `files.insertions`                                                                    as insertion,
             `files.lines`                                                                         as lines,
             splitByChar('/', file_name)                                                           as dir_list
              ,
             if(length(dir_list) > 2, concat(dir_list[1], '/'), '')                                as dir_level_1,
             if(length(dir_list) > 3, concat(dir_list[1], '/', dir_list[2], '/', dir_list[3]), '') as dir_level_3
      from (select *
            from gits array join files.lines, `files.file_name`, `files.insertions`, `files.deletions`
            where search_key__owner = 'pytorch'
              and search_key__repo = 'pytorch'
--               and lower(message) like '%ascend%'
              and length(parents) = 1

              and (author_email global in (select author_email
                                           from (select author_email, count() as commit_count
                                                 from gits
                                                 where search_key__owner = 'Ascend'
                                                   and search_key__repo = 'pytorch'
                                                   and author_email global not in
                                                       ('huawei_ci_bot@163.com',
                                                        'pta_robot@163.com'
                                                           )
-- and splitByChar('@',author_email)[2] global in ('huawei.com','hisilicon.com','h-partners.com')
                                                 group by author_email
                                                 having commit_count > 1
                                                 order by commit_count desc))
                or splitByChar('@', author_email)[2] global in ('huawei.com', 'hisilicon.com', 'h-partners.com')))
      where dir_level_1 != ''
--         and (file_name like '%test%' or file_name like '%.md')
        and lower(file_name) like  '%ascend%'
        and dir_level_3 != '')
group by dir_level_3

;


select 'huawei' as company,year, count() as commit_count
from gits
where search_key__owner = 'pytorch'
  and search_key__repo = 'pytorch'
--               and lower(message) like '%ascend%'
  and length(parents) = 1

  and (author_email global in (select author_email
                               from (select author_email, count() as commit_count
                                     from gits
                                     where search_key__owner = 'Ascend'
                                       and search_key__repo = 'pytorch'
                                       and author_email global not in
                                           ('huawei_ci_bot@163.com',
                                            'pta_robot@163.com'
                                               )
-- and splitByChar('@',author_email)[2] global in ('huawei.com','hisilicon.com','h-partners.com')
                                     group by author_email
                                     having commit_count > 1
                                     order by commit_count desc))
    or splitByChar('@', author_email)[2] global in ('huawei.com', 'hisilicon.com', 'h-partners.com'))
group by search_key__owner, search_key__repo, toYear(authored_date) as year
order by year


-- huawei 人数


select search_key__owner, search_key__repo,author_email,count() as commit_count
from gits
where search_key__owner = 'pytorch'
  and search_key__repo = 'pytorch'
--               and lower(message) like '%ascend%'
  and length(parents) = 1

  and (author_email global in (select author_email
                               from (select author_email, count() as commit_count
                                     from gits
                                     where search_key__owner = 'Ascend'
                                       and search_key__repo = 'pytorch'
                                       and author_email global not in
                                           ('huawei_ci_bot@163.com',
                                            'pta_robot@163.com'
                                               )
-- and splitByChar('@',author_email)[2] global in ('huawei.com','hisilicon.com','h-partners.com')
                                     group by author_email
                                     having commit_count > 1
                                     order by commit_count desc))
    or splitByChar('@', author_email)[2] global in ('huawei.com', 'hisilicon.com', 'h-partners.com'))
group by search_key__owner, search_key__repo,author_email








select author_email, count() as commit_count
                                     from gits
                                     where search_key__owner = 'Ascend'
                                       and search_key__repo = 'pytorch'
                                       and author_email global not in
                                           ('huawei_ci_bot@163.com',
                                            'pta_robot@163.com'
                                               )
                                      and author_email global in ('18207133434@163.com','ljw1101.vip@gmail.com')
-- and splitByChar('@',author_email)[2] global in ('huawei.com','hisilicon.com','h-partners.com')
                                     group by author_email
                                     having commit_count > 1
                                     order by commit_count desc







;
-- 指定目录1层
select search_key__owner,
       search_key__repo,
       company,
       year,
       dir_level_1                    as dir_hierarchy_1,
       count(distinct hexsha)         as commit_count,
       count(distinct lower(user_id)) as developer_count,
       sum(insertions)                as total_insertions,
       sum(deletions)                 as total_deletions,
       sum(lines)                     as total_lines
from (select splitByChar('/', file_name)                                         as dir_list
              ,
             if(length(dir_list) > 1, concat(dir_list[1], '/'), '')              as dir_level_1,
             *
      from (select *, if(author__login = '', author_name, author__login) as user_id
            from (select search_key__owner,
                         search_key__repo,
                         hexsha,
                         author_name,
                         author_email,
                         parents,
                         file_name,
                         insertions,
                         deletions,
                         lines,
                         year,
                         b.company as company,
                         author__login
                  from (select *
                        from (select a.*, b.company, b.author__login
                              from (select search_key__owner,
                                           search_key__repo,
                                           hexsha,
                                           author_name,
                                           author_email,
                                           parents,
                                           `files.file_name`     as file_name,
                                           `files.insertions`       insertions,
                                           `files.deletions`        deletions,
                                           `files.lines`            lines,
                                           toYear(authored_date) as year
                                    from gits array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`
                                    where search_key__owner = 'pytorch'
                                      and search_key__repo = 'pytorch'
                                      and (file_name like 'torch/%' or file_name like 'aten/%' or
                                           file_name like 'c10%' or file_name like 'caffe2/%' or
                                           file_name like 'functorch/%')
                                      and file_name not like '%{%'

--                                 and file_name not like '%test%'
--                                 and file_name not like '%.md'
--                                 and file_name not like '%examples%'
                                      and author_name != 'PyTorch MergeBot') as a global
                                       left join (select sha, company, author__login
                                                  from commit_company
                                                  where search_key__owner = 'pytorch'
                                                    and search_key__repo = 'pytorch'
                                                  group by sha, company, author__login) as b on a.hexsha = b.sha)
                        where company = '') as a global
                           left join (select * from company_email_map) as b
                                     on splitByChar('@', a.author_email)[2] = b.email_domain

                  union all
                  select *
                  from (select a.*, b.company, b.author__login
                        from (select search_key__owner,
                                     search_key__repo,
                                     hexsha,
                                     author_name,
                                     author_email,
                                     parents,
                                     `files.file_name`     as file_name,
                                     `files.insertions`       insertions,
                                     `files.deletions`        deletions,
                                     `files.lines`            lines,
                                     toYear(authored_date) as year
                              from gits array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`
                              where search_key__owner = 'pytorch'
                                and search_key__repo = 'pytorch'
                                and (file_name like 'torch/%' or file_name like 'aten/%' or file_name like 'c10%' or
                                     file_name like 'caffe2/%' or file_name like 'functorch/%')
                                and file_name not like '%{%'
--                           and file_name not like '%test%'
--                           and file_name not like '%.md'
--                           and file_name not like '%examples%'
                                and author_name != 'PyTorch MergeBot') as a global
                                 left join (select sha, company, author__login
                                            from commit_company
                                            where search_key__owner = 'pytorch'
                                              and search_key__repo = 'pytorch'
                                            group by sha, company, author__login) as b on a.hexsha = b.sha)
                  where company != '')))
where company global in ('google',
                         'meta',
                         'twitter',
                         'lightning.ai',
                         'naver',
                         'nvidia',
                         'microsoft',
                         'ibm',
                         'quansight',
                         'fujitsu',
                         'amd',
                         'openai',
                         'intel',
                         'amazon',
                         'huawei',
                         'tencent',
                         'apple',
                         'octoml',
                         'bytedance',
                         'alibaba')
  and dir_hierarchy_1 != ''
group by search_key__owner, search_key__repo, company, year, dir_level_1
order by year, dir_hierarchy_1, commit_count desc


--               and toYYYYMMDD(authored_date) <= 20210616
--               and toYYYYMMDD(authored_date) > 20200422
;
select *
from commit_company
 limit 1
;
insert into table company_email_map values ('twitter.com','twitter',toUnixTimestamp(now()))
;
select * from company_email_map where email_domain ='twitter.com'
;

select author__login from github_commits
                     where commit__author__email = 'davidriazati@fb.com'

;
--pta_robot@163.com
select commit__author__email, count()
from github_commits
where search_key__owner = 'Ascend'
  and search_key__repo = 'pytorch'
  and author__login not like '%robot%'
  and commit__author__email not like '%robot%'

group by commit__author__email
order by count() desc

;
select author__login, count()
from github_commits
where search_key__owner = 'Ascend'
  and search_key__repo = 'pytorch'
  and author__login not like '%robot%'
  and commit__author__email = 'pta_robot@163.com'
group by author__login
order by count() desc

;

select github_login from github_id_main_tz_map_v2 where inferred_area = '北美' limit 10
;
select author__login
from github_commits
where (search_key__owner = 'triton-lang'
  and search_key__repo = 'triton' or
       search_key__owner = 'pytorch' and search_key__repo = 'pytorch')
  and author__login like '%nvidia%'
group by author__login
;
select user__login
from github_issues
where (search_key__owner = 'triton-lang' and search_key__repo = 'triton' or
       search_key__owner = 'pytorch' and search_key__repo = 'pytorch')
  and user__login like '%openai%'
group by user__login

-- approver 所属公司
select a.*,
       if(login global in
          ('galipremsagar', 'kkraus14', 'nithinraok',
           'XuesongYang',
           'codereport',
           'dillon-cullinan', 'thomcom',
           'charlesbluca', 'jnke2016', 'naimnv'), 'nvidia',
          b.company) as company
from (select search_key__owner,
             search_key__repo,
             login,
             count() as approved_count
      from (select search_key__owner,
                   search_key__repo,
                   JSONExtractString(timeline_raw, 'id')                               as id,
                   JSONExtractString(timeline_raw, 'state')                            as state,
                   JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'login') as login
            from github_issues_timeline
            where search_key__event = 'reviewed'
              and (search_key__owner = 'Project-MONAI' and
                   search_key__repo = 'MONAI' or
                   search_key__owner = 'NVIDIA' and
                   search_key__repo = 'NeMo' or
                   search_key__owner = 'NVIDIA' and
                   search_key__repo = 'NeMo-Guardrails' or
                   search_key__owner = 'onnx' and
                   search_key__repo = 'onnx-tensorrt' or
                   search_key__owner = 'NVIDIA' and
                   search_key__repo = 'TransformerEngine' or
                   search_key__owner = 'pytorch' and
                   search_key__repo = 'TensorRT' or
                   search_key__owner = 'NVIDIA-Omniverse' and
                   search_key__repo = 'PhysX' or
                   search_key__owner =
                   'PixarAnimationStudios' and
                   search_key__repo = 'USD' or
                   search_key__owner = 'NVIDIA-Omniverse' and
                   search_key__repo = 'USD-proposals' or
                   search_key__owner = 'NVIDIA' and
                   search_key__repo = 'AMGX' or
                   search_key__owner = 'rapidsai' and
                   search_key__repo = 'rmm' or
                   search_key__owner = 'rapidsai' and
                   search_key__repo = 'raft' or
                   search_key__owner = 'rapidsai' and
                   search_key__repo = 'cuxfilter' or
                   search_key__owner = 'rapidsai' and
                   search_key__repo = 'cugraph' or
                   search_key__owner = 'rapidsai' and
                   search_key__repo = 'cudf' or
                   search_key__owner = 'CVCUDA' and
                   search_key__repo = 'CV-CUDA' or
                   search_key__owner = 'NVIDIA' and
                   search_key__repo = 'Megatron-LM')
              and (state = 'approved' or state = 'changes_requested' or state = 'dismissed')
              and login global not in ('GPUtester',
                                       'github-actions[bot]',
                                       'github-actions[bot]',
                                       'rapids-bot[bot]',
                                       'monai-bot',
                                       'dependabot[bot]',
                                       'deepsource-autofix[bot]',
                                       'pre-commit-ci[bot]',
                                       'probottest',
                                       'lgtm-com[bot]',
                                       'facebook-github-bot',
                                       'smartestrobotdai',
                                       'fbottau',
                                       'probottest')
            group by search_key__owner, search_key__repo, id, state, login)
      group by search_key__owner, search_key__repo, login
      having approved_count > 10
      order by search_key__owner, search_key__repo, approved_count desc
         ) as a global
         left join (select *
                    from (select author__login, company
                          from (select author__login, company
                                from commit_company
                                where (search_key__owner =
                                       'Project-MONAI' and
                                       search_key__repo =
                                       'MONAI' or
                                       search_key__owner =
                                       'NVIDIA' and
                                       search_key__repo =
                                       'NeMo' or
                                       search_key__owner =
                                       'NVIDIA' and
                                       search_key__repo =
                                       'NeMo-Guardrails' or
                                       search_key__owner =
                                       'onnx' and
                                       search_key__repo =
                                       'onnx-tensorrt' or
                                       search_key__owner =
                                       'NVIDIA' and
                                       search_key__repo =
                                       'TransformerEngine' or
                                       search_key__owner =
                                       'pytorch' and
                                       search_key__repo =
                                       'TensorRT' or
                                       search_key__owner =
                                       'NVIDIA-Omniverse' and
                                       search_key__repo =
                                       'PhysX' or
                                       search_key__owner =
                                       'PixarAnimationStudios' and
                                       search_key__repo =
                                       'USD' or
                                       search_key__owner =
                                       'NVIDIA-Omniverse' and
                                       search_key__repo =
                                       'USD-proposals' or
                                       search_key__owner =
                                       'NVIDIA' and
                                       search_key__repo =
                                       'AMGX' or
                                       search_key__owner =
                                       'rapidsai' and
                                       search_key__repo =
                                       'rmm' or
                                       search_key__owner =
                                       'rapidsai' and
                                       search_key__repo =
                                       'raft' or
                                       search_key__owner =
                                       'rapidsai' and
                                       search_key__repo =
                                       'cuxfilter' or
                                       search_key__owner =
                                       'rapidsai' and
                                       search_key__repo =
                                       'cugraph' or
                                       search_key__owner =
                                       'rapidsai' and
                                       search_key__repo =
                                       'cudf' or
                                       search_key__owner =
                                       'CVCUDA' and
                                       search_key__repo =
                                       'CV-CUDA' or
                                       search_key__owner =
                                       'NVIDIA' and
                                       search_key__repo =
                                       'Megatron-LM')
                                  and author__login != ''
                                  and commit_company.company != ''
                                group by author__login, company
                                union all
                                select login,
                                       if(
                                               final_company_inferred_from_company =
                                               'facebook',
                                               'meta',
                                               final_company_inferred_from_company) as company
                                from github_profile
                                where github_profile.final_company_inferred_from_company != ''
                                  and final_company_inferred_from_company != '无'
                                group by login, final_company_inferred_from_company)
                          group by author__login, company
                          order by author__login)
                    where author__login != '') as b
                   on a.login = b.author__login
order by search_key__owner, search_key__repo, approved_count desc
limit 30 by search_key__repo

-- triton-cpu 与主项目之间的差异
select commit_company.search_key__owner,
       commit_company.search_key__repo,
       concat(search_key__owner, '__', search_key__repo) as owner_repo,
       if(company = '', '其他', company)                 as company,
       count()                                              commit_count
from commit_company
where (search_key__owner = 'triton-lang'
    and search_key__repo = 'triton-cpu')
  and sha global in (select hexsha
                     from gits
                     where search_key__owner = 'triton-lang'
                       and search_key__repo = 'triton-cpu'
                       and hexsha global not in
                           (select hexsha
                            from gits
                            where search_key__owner = 'triton-lang'
                              and search_key__repo = 'triton'
                            group by hexsha)
                     group by hexsha)
group by commit_company.search_key__owner, commit_company.search_key__repo, company
order by commit_company.search_key__owner, commit_company.search_key__repo, commit_count desc



-- 是否被approver评论
select search_key__owner,
       search_key__repo,
       countIf(is_be_commented_by_approver = 1) approver_commented,
       countIf(is_be_commented_by_approver = 0) no_approver_commented
from (select search_key__owner, search_key__repo, number, is_be_commented_by_approver
      from (select search_key__owner,
                   search_key__repo,
                   number,
                   open_issue_user_login,
                   issue_year,
                   closed_at_day,
                   state,
                   if(issues_comment_login != '', 1, 0) as is_be_commented_by_approver
            from (select search_key__owner,
                         search_key__repo,
                         number,
                         a.user__login as open_issue_user_login,
                         a.year        as issue_year,
                         closed_at_day,
                         state,
                         b.user__login as issues_comment_login,
                         b.year        as issue_comment_year
                  from (select a.*, if(b.login != '', 'yes', 'no') is_approver
                        from (select a.*,
                                     if(user__login global in
                                        ('galipremsagar', 'kkraus14', 'nithinraok', 'XuesongYang', 'codereport',
                                         'dillon-cullinan',
                                         'thomcom',
                                         'charlesbluca', 'jnke2016', 'naimnv'), 'nvidia', b.company) as company
                              from (select search_key__owner,
                                           search_key__repo,
                                           number,
                                           id,
                                           user__login,
                                           pull_request__url,
                                           toYear(created_at) as year,
                                           toYYYYMMDD(closed_at) closed_at_day,
                                           state
                                    from github_issues
                                    where (search_key__owner = 'Project-MONAI' and search_key__repo = 'MONAI' or
                                           search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo' or
                                           search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo-Guardrails' or
                                           search_key__owner = 'onnx' and search_key__repo = 'onnx-tensorrt' or
                                           search_key__owner = 'NVIDIA' and
                                           search_key__repo = 'TransformerEngine' or
                                           search_key__owner = 'pytorch' and search_key__repo = 'TensorRT' or
                                           search_key__owner = 'NVIDIA-Omniverse' and search_key__repo = 'PhysX' or
                                           search_key__owner = 'PixarAnimationStudios' and
                                           search_key__repo = 'USD' or search_key__owner = 'NVIDIA-Omniverse' and
                                                                       search_key__repo = 'USD-proposals' or
                                           search_key__owner = 'NVIDIA' and search_key__repo = 'AMGX' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'rmm' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'raft' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'cuxfilter' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'cugraph' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'cudf' or
                                           search_key__owner = 'CVCUDA' and search_key__repo = 'CV-CUDA' or
                                           search_key__owner = 'NVIDIA' and search_key__repo = 'Megatron-LM')
                                      and pull_request__url = ''
                                      and user__login global not in ('GPUtester',
                                                                     'github-actions[bot]',
                                                                     'github-actions[bot]',
                                                                     'rapids-bot[bot]',
                                                                     'monai-bot',
                                                                     'dependabot[bot]',
                                                                     'deepsource-autofix[bot]',
                                                                     'pre-commit-ci[bot]',
                                                                     'probottest',
                                                                     'lgtm-com[bot]',
                                                                     'facebook-github-bot',
                                                                     'smartestrobotdai',
                                                                     'fbottau',
                                                                     'probottest')
                                    group by search_key__owner, search_key__repo, id, pull_request__url,
                                             user__login, year, closed_at, state, number) as a global
                                       left join (select author__login, company
                                                  from (select author__login, company
                                                        from commit_company
                                                        where (search_key__owner = 'Project-MONAI' and
                                                               search_key__repo = 'MONAI' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'NeMo' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'NeMo-Guardrails' or
                                                               search_key__owner = 'onnx' and
                                                               search_key__repo = 'onnx-tensorrt' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'TransformerEngine' or
                                                               search_key__owner = 'pytorch' and
                                                               search_key__repo = 'TensorRT' or
                                                               search_key__owner = 'NVIDIA-Omniverse' and
                                                               search_key__repo = 'PhysX' or
                                                               search_key__owner = 'PixarAnimationStudios' and
                                                               search_key__repo = 'USD' or
                                                               search_key__owner = 'NVIDIA-Omniverse' and
                                                               search_key__repo = 'USD-proposals' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'AMGX' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'rmm' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'raft' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'cuxfilter' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'cugraph' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'cudf' or
                                                               search_key__owner = 'CVCUDA' and
                                                               search_key__repo = 'CV-CUDA' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'Megatron-LM')
                                                          and author__login != ''
                                                          and commit_company.company != ''
                                                        group by author__login, company
                                                        union all
                                                        select login,
                                                               if(final_company_inferred_from_company =
                                                                  'facebook',
                                                                  'meta',
                                                                  final_company_inferred_from_company) as company
                                                        from github_profile
                                                        where github_profile.final_company_inferred_from_company != ''
                                                          and final_company_inferred_from_company != '无'
                                                        group by login, final_company_inferred_from_company)
                                                  group by author__login, company
                                                  order by author__login) as b on a.user__login = b.author__login
                              where company != 'apache_org') as a global
                                 left join (select *
                                            from (select a.*,
                                                         if(login global in
                                                            ('galipremsagar', 'kkraus14', 'nithinraok', 'XuesongYang',
                                                             'codereport',
                                                             'dillon-cullinan', 'thomcom',
                                                             'charlesbluca', 'jnke2016', 'naimnv'), 'nvidia',
                                                            b.company) as company
                                                  from (select search_key__owner,
                                                               search_key__repo,
                                                               login,
                                                               count() as approved_count
                                                        from (select search_key__owner,
                                                                     search_key__repo,
                                                                     JSONExtractString(timeline_raw, 'id')                               as id,
                                                                     JSONExtractString(timeline_raw, 'state')                            as state,
                                                                     JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'login') as login
                                                              from github_issues_timeline
                                                              where search_key__event = 'reviewed'
                                                                and (search_key__owner = 'Project-MONAI' and
                                                                     search_key__repo = 'MONAI' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'NeMo' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'NeMo-Guardrails' or
                                                                     search_key__owner = 'onnx' and
                                                                     search_key__repo = 'onnx-tensorrt' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'TransformerEngine' or
                                                                     search_key__owner = 'pytorch' and
                                                                     search_key__repo = 'TensorRT' or
                                                                     search_key__owner = 'NVIDIA-Omniverse' and
                                                                     search_key__repo = 'PhysX' or
                                                                     search_key__owner = 'PixarAnimationStudios' and
                                                                     search_key__repo = 'USD' or
                                                                     search_key__owner = 'NVIDIA-Omniverse' and
                                                                     search_key__repo = 'USD-proposals' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'AMGX' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'rmm' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'raft' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'cuxfilter' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'cugraph' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'cudf' or
                                                                     search_key__owner = 'CVCUDA' and
                                                                     search_key__repo = 'CV-CUDA' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'Megatron-LM')
                                                                and (state = 'approved' or state = 'changes_requested' or state = 'dismissed')
                                                                and login global not in ('GPUtester',
                                                                                         'github-actions[bot]',
                                                                                         'github-actions[bot]',
                                                                                         'rapids-bot[bot]',
                                                                                         'monai-bot',
                                                                                         'dependabot[bot]',
                                                                                         'deepsource-autofix[bot]',
                                                                                         'pre-commit-ci[bot]',
                                                                                         'probottest',
                                                                                         'lgtm-com[bot]',
                                                                                         'facebook-github-bot',
                                                                                         'smartestrobotdai',
                                                                                         'fbottau',
                                                                                         'probottest')
                                                              group by search_key__owner, search_key__repo, id, state, login)
                                                        group by search_key__owner, search_key__repo, login
                                                        having approved_count > 10
                                                        order by search_key__owner, search_key__repo, approved_count desc
                                                           ) as a global
                                                           left join (select *
                                                                      from (select author__login, company
                                                                            from (select author__login, company
                                                                                  from commit_company
                                                                                  where (search_key__owner =
                                                                                         'Project-MONAI' and
                                                                                         search_key__repo = 'MONAI' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo = 'NeMo' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo =
                                                                                         'NeMo-Guardrails' or
                                                                                         search_key__owner = 'onnx' and
                                                                                         search_key__repo =
                                                                                         'onnx-tensorrt' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo =
                                                                                         'TransformerEngine' or
                                                                                         search_key__owner =
                                                                                         'pytorch' and
                                                                                         search_key__repo =
                                                                                         'TensorRT' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA-Omniverse' and
                                                                                         search_key__repo = 'PhysX' or
                                                                                         search_key__owner =
                                                                                         'PixarAnimationStudios' and
                                                                                         search_key__repo = 'USD' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA-Omniverse' and
                                                                                         search_key__repo =
                                                                                         'USD-proposals' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo = 'AMGX' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo = 'rmm' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo = 'raft' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo =
                                                                                         'cuxfilter' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo = 'cugraph' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo = 'cudf' or
                                                                                         search_key__owner =
                                                                                         'CVCUDA' and
                                                                                         search_key__repo = 'CV-CUDA' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo =
                                                                                         'Megatron-LM')
                                                                                    and author__login != ''
                                                                                    and commit_company.company != ''
                                                                                  group by author__login, company
                                                                                  union all
                                                                                  select login,
                                                                                         if(
                                                                                                 final_company_inferred_from_company =
                                                                                                 'facebook',
                                                                                                 'meta',
                                                                                                 final_company_inferred_from_company) as company
                                                                                  from github_profile
                                                                                  where github_profile.final_company_inferred_from_company != ''
                                                                                    and final_company_inferred_from_company != '无'
                                                                                  group by login, final_company_inferred_from_company)
                                                                            group by author__login, company
                                                                            order by author__login)
                                                                      where author__login != '') as b
                                                                     on a.login = b.author__login
                                                  order by search_key__owner, search_key__repo, approved_count desc
                                                  limit 30 by search_key__repo)
                                            where company = 'nvidia') as b
                                           on a.search_key__owner = b.search_key__owner and
                                              a.search_key__repo = b.search_key__repo and
                                              a.user__login = b.login
                        where company != 'nvidia'
                          and is_approver = 'no') as a global
                           left join (select *
                                      from (select search_key__owner,
                                                   search_key__repo,
                                                   id,
                                                   user__login,
                                                   toYear(created_at) as year,
                                                   search_key__number
                                            from github_issues_comments
                                            where (search_key__owner = 'Project-MONAI' and search_key__repo = 'MONAI' or
                                                   search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo' or
                                                   search_key__owner = 'NVIDIA' and
                                                   search_key__repo = 'NeMo-Guardrails' or
                                                   search_key__owner = 'onnx' and search_key__repo = 'onnx-tensorrt' or
                                                   search_key__owner = 'NVIDIA' and
                                                   search_key__repo = 'TransformerEngine' or
                                                   search_key__owner = 'pytorch' and search_key__repo = 'TensorRT' or
                                                   search_key__owner = 'NVIDIA-Omniverse' and
                                                   search_key__repo = 'PhysX' or
                                                   search_key__owner = 'PixarAnimationStudios' and
                                                   search_key__repo = 'USD' or
                                                   search_key__owner = 'NVIDIA-Omniverse' and
                                                   search_key__repo = 'USD-proposals' or
                                                   search_key__owner = 'NVIDIA' and search_key__repo = 'AMGX' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'rmm' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'raft' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'cuxfilter' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'cugraph' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'cudf' or
                                                   search_key__owner = 'CVCUDA' and search_key__repo = 'CV-CUDA' or
                                                   search_key__owner = 'NVIDIA' and search_key__repo = 'Megatron-LM')
                                              and user__login global not in ('GPUtester',
                                                                             'github-actions[bot]',
                                                                             'github-actions[bot]',
                                                                             'rapids-bot[bot]',
                                                                             'monai-bot',
                                                                             'dependabot[bot]',
                                                                             'deepsource-autofix[bot]',
                                                                             'pre-commit-ci[bot]',
                                                                             'probottest',
                                                                             'lgtm-com[bot]',
                                                                             'facebook-github-bot',
                                                                             'smartestrobotdai',
                                                                             'fbottau',
                                                                             'probottest')
                                            group by search_key__owner, search_key__repo, id, user__login,
                                                     year,
                                                     search_key__number) as a global
                                               join (select *
                                                     from (select a.*,
                                                                  if(login global in
                                                                     ('galipremsagar', 'kkraus14', 'nithinraok',
                                                                      'XuesongYang',
                                                                      'codereport',
                                                                      'dillon-cullinan', 'thomcom',
                                                                      'charlesbluca', 'jnke2016', 'naimnv'), 'nvidia',
                                                                     b.company) as company
                                                           from (select search_key__owner,
                                                                        search_key__repo,
                                                                        login,
                                                                        count() as approved_count
                                                                 from (select search_key__owner,
                                                                              search_key__repo,
                                                                              JSONExtractString(timeline_raw, 'id')                               as id,
                                                                              JSONExtractString(timeline_raw, 'state')                            as state,
                                                                              JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'login') as login
                                                                       from github_issues_timeline
                                                                       where search_key__event = 'reviewed'
                                                                         and (search_key__owner = 'Project-MONAI' and
                                                                              search_key__repo = 'MONAI' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'NeMo' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'NeMo-Guardrails' or
                                                                              search_key__owner = 'onnx' and
                                                                              search_key__repo = 'onnx-tensorrt' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'TransformerEngine' or
                                                                              search_key__owner = 'pytorch' and
                                                                              search_key__repo = 'TensorRT' or
                                                                              search_key__owner = 'NVIDIA-Omniverse' and
                                                                              search_key__repo = 'PhysX' or
                                                                              search_key__owner =
                                                                              'PixarAnimationStudios' and
                                                                              search_key__repo = 'USD' or
                                                                              search_key__owner = 'NVIDIA-Omniverse' and
                                                                              search_key__repo = 'USD-proposals' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'AMGX' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'rmm' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'raft' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'cuxfilter' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'cugraph' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'cudf' or
                                                                              search_key__owner = 'CVCUDA' and
                                                                              search_key__repo = 'CV-CUDA' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'Megatron-LM')
                                                                         and (state = 'approved' or state = 'changes_requested' or state = 'dismissed')
                                                                         and login global not in ('GPUtester',
                                                                                                  'github-actions[bot]',
                                                                                                  'github-actions[bot]',
                                                                                                  'rapids-bot[bot]',
                                                                                                  'monai-bot',
                                                                                                  'dependabot[bot]',
                                                                                                  'deepsource-autofix[bot]',
                                                                                                  'pre-commit-ci[bot]',
                                                                                                  'probottest',
                                                                                                  'lgtm-com[bot]',
                                                                                                  'facebook-github-bot',
                                                                                                  'smartestrobotdai',
                                                                                                  'fbottau',
                                                                                                  'probottest')
                                                                       group by search_key__owner, search_key__repo, id, state, login)
                                                                 group by search_key__owner, search_key__repo, login
                                                                 having approved_count > 10
                                                                 order by search_key__owner, search_key__repo, approved_count desc
                                                                    ) as a global
                                                                    left join (select *
                                                                               from (select author__login, company
                                                                                     from (select author__login, company
                                                                                           from commit_company
                                                                                           where (search_key__owner =
                                                                                                  'Project-MONAI' and
                                                                                                  search_key__repo =
                                                                                                  'MONAI' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'NeMo' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'NeMo-Guardrails' or
                                                                                                  search_key__owner =
                                                                                                  'onnx' and
                                                                                                  search_key__repo =
                                                                                                  'onnx-tensorrt' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'TransformerEngine' or
                                                                                                  search_key__owner =
                                                                                                  'pytorch' and
                                                                                                  search_key__repo =
                                                                                                  'TensorRT' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA-Omniverse' and
                                                                                                  search_key__repo =
                                                                                                  'PhysX' or
                                                                                                  search_key__owner =
                                                                                                  'PixarAnimationStudios' and
                                                                                                  search_key__repo =
                                                                                                  'USD' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA-Omniverse' and
                                                                                                  search_key__repo =
                                                                                                  'USD-proposals' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'AMGX' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'rmm' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'raft' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'cuxfilter' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'cugraph' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'cudf' or
                                                                                                  search_key__owner =
                                                                                                  'CVCUDA' and
                                                                                                  search_key__repo =
                                                                                                  'CV-CUDA' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'Megatron-LM')
                                                                                             and author__login != ''
                                                                                             and commit_company.company != ''
                                                                                           group by author__login, company
                                                                                           union all
                                                                                           select login,
                                                                                                  if(
                                                                                                          final_company_inferred_from_company =
                                                                                                          'facebook',
                                                                                                          'meta',
                                                                                                          final_company_inferred_from_company) as company
                                                                                           from github_profile
                                                                                           where github_profile.final_company_inferred_from_company != ''
                                                                                             and final_company_inferred_from_company != '无'
                                                                                           group by login, final_company_inferred_from_company)
                                                                                     group by author__login, company
                                                                                     order by author__login)
                                                                               where author__login != '') as b
                                                                              on a.login = b.author__login
                                                           order by search_key__owner, search_key__repo, approved_count desc
                                                           limit 30 by search_key__repo)
                                                     where company = 'nvidia') as b
                                                    on a.search_key__owner = b.search_key__owner and
                                                       a.search_key__repo = b.search_key__repo and
                                                       a.user__login = b.login
                      ) as b on a.search_key__owner = b.search_key__owner and
                                a.search_key__repo = b.search_key__repo and
                                a.number = b.search_key__number
                  where open_issue_user_login != issues_comment_login))
      group by search_key__owner, search_key__repo, number, is_be_commented_by_approver)
group by search_key__owner, search_key__repo





-- nvidia项目 被拒绝合入占比 整体
select search_key__owner,
       search_key__repo,
       if(company != 'nvidia', '其他', company)           as company,
       countIf(is_be_accepted = 'accepted')                  pr_merged_count,
       countIf(is_be_accepted = 'rejected')                  pr_rejected_count,
       countIf(is_be_accepted != 'open')                  as pr_close_count,
       round(pr_rejected_count / pr_close_count * 100, 1) as pr_rejected_percentage
from (select a.*,
             if(user__login global in
                ('galipremsagar', 'kkraus14', 'nithinraok', 'XuesongYang', 'codereport', 'dillon-cullinan', 'thomcom',
                 'charlesbluca'), 'nvidia', b.company) as company
      from (
               select search_key__owner,
                      search_key__repo,
                      id,
                      user__login,
                      pull_request__url,
                      toYear(created_at) as               year,
                      toYYYYMMDD(pull_request__merged_at) merged_at_day,
                      toYYYYMMDD(closed_at)               closed_at_day,
                      state,
                      multiIf(state = 'closed' and merged_at_day = 19700101, 'rejected',
                              state = 'closed' and merged_at_day != 19700101, 'accepted',
                              'open')                     is_be_accepted
               from github_issues
               where (search_key__owner = 'Project-MONAI' and search_key__repo = 'MONAI' or
                      search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo' or
                      search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo-Guardrails' or
                      search_key__owner = 'onnx' and search_key__repo = 'onnx-tensorrt' or
                      search_key__owner = 'NVIDIA' and
                      search_key__repo = 'TransformerEngine' or
                      search_key__owner = 'pytorch' and search_key__repo = 'TensorRT' or
                      search_key__owner = 'NVIDIA-Omniverse' and search_key__repo = 'PhysX' or
                      search_key__owner = 'PixarAnimationStudios' and
                      search_key__repo = 'USD' or search_key__owner = 'NVIDIA-Omniverse' and
                                                  search_key__repo = 'USD-proposals' or
                      search_key__owner = 'NVIDIA' and search_key__repo = 'AMGX' or
                      search_key__owner = 'rapidsai' and search_key__repo = 'rmm' or
                      search_key__owner = 'rapidsai' and search_key__repo = 'raft' or
                      search_key__owner = 'rapidsai' and search_key__repo = 'cuxfilter' or
                      search_key__owner = 'rapidsai' and search_key__repo = 'cugraph' or
                      search_key__owner = 'rapidsai' and search_key__repo = 'cudf' or
                      search_key__owner = 'CVCUDA' and search_key__repo = 'CV-CUDA' or
                      search_key__owner = 'NVIDIA' and search_key__repo = 'Megatron-LM')
                 and pull_request__url != ''
                 and user__login global not in ('GPUtester',
                                                'github-actions[bot]',
                                                'github-actions[bot]',
                                                'rapids-bot[bot]',
                                                'monai-bot',
                                                'dependabot[bot]',
                                                'deepsource-autofix[bot]',
                                                'pre-commit-ci[bot]',
                                                'probottest',
                                                'lgtm-com[bot]',
                                                'facebook-github-bot',
                                                'smartestrobotdai',
                                                'fbottau',
                                                'probottest')
               group by search_key__owner, search_key__repo, id, pull_request__url,
                        user__login, year, pull_request__merged_at, closed_at, state
               ) as a global
               left join (select author__login, company
                          from (select author__login, company
                                from commit_company
                                where (search_key__owner = 'Project-MONAI' and search_key__repo = 'MONAI' or
                                       search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo' or
                                       search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo-Guardrails' or
                                       search_key__owner = 'onnx' and search_key__repo = 'onnx-tensorrt' or
                                       search_key__owner = 'NVIDIA' and
                                       search_key__repo = 'TransformerEngine' or
                                       search_key__owner = 'pytorch' and search_key__repo = 'TensorRT' or
                                       search_key__owner = 'NVIDIA-Omniverse' and search_key__repo = 'PhysX' or
                                       search_key__owner = 'PixarAnimationStudios' and
                                       search_key__repo = 'USD' or search_key__owner = 'NVIDIA-Omniverse' and
                                                                   search_key__repo = 'USD-proposals' or
                                       search_key__owner = 'NVIDIA' and search_key__repo = 'AMGX' or
                                       search_key__owner = 'rapidsai' and search_key__repo = 'rmm' or
                                       search_key__owner = 'rapidsai' and search_key__repo = 'raft' or
                                       search_key__owner = 'rapidsai' and search_key__repo = 'cuxfilter' or
                                       search_key__owner = 'rapidsai' and search_key__repo = 'cugraph' or
                                       search_key__owner = 'rapidsai' and search_key__repo = 'cudf' or
                                       search_key__owner = 'CVCUDA' and search_key__repo = 'CV-CUDA' or
                                       search_key__owner = 'NVIDIA' and search_key__repo = 'Megatron-LM')
                                  and author__login != ''
                                  and commit_company.company != ''
                                group by author__login, company
                                union all
                                select login,
                                       if(final_company_inferred_from_company =
                                          'facebook',
                                          'meta',
                                          final_company_inferred_from_company) as company
                                from github_profile
                                where github_profile.final_company_inferred_from_company != ''
                                  and final_company_inferred_from_company != '无'
                                group by login, final_company_inferred_from_company)
                          group by author__login, company
                          order by author__login) as b on a.user__login = b.author__login)
group by search_key__owner, search_key__repo, company
order by search_key__owner, search_key__repo





-- issues count，pr count  ，issues comment count pr多少被合入， pr多少被拒绝， 回复了多少issues，commit count  ，年份，total deletions  total lines  developer count
-- triton 公司 全量 ,cpu 与主项目不同的地方 带年份


select if(a.owner = '', b.owner, a.owner)       as owner,
       if(a.repo = '', b.repo, a.repo)          as repo,
              concat(owner, '__', repo)                      as owner_repo,

       if(a.company = '', b.company, a.company) as company,
       if(a.year = 0, b.year, a.year)           as year,
       issues_count,
       pr_count,
       issue_comment_count,
       pr_merged_count,
       pr_rejected_count,
       commented_issues_count,
       commit_count,
       total_insertions,
       total_deletions,
       total_lines,
       developer_count
from (select if(a.owner = '', b.search_key__owner, a.owner) as owner,
       if(a.repo = '', b.search_key__repo, a.repo)    as repo,
       if(a.company = '', b.company, a.company)       as company,
       if(a.year = 0, b.year, a.year)                 as year,
       concat(owner, '__', repo)                      as owner_repo,
       issues_count,
       pr_count,
       issue_comment_count,
       commit_count,
       total_insertions,
       total_deletions,
       total_lines,
       developer_count

from (select if(a.owner = '', b.search_key__owner, a.owner) as owner,
             if(a.repo = '', b.search_key__repo, a.repo)    as repo,
             if(a.company = '', b.company, a.company)       as company,
             if(a.year = 0, b.year, a.year)                 as year,
             issues_count,
             pr_count,
             issue_comment_count
      from (select if(a.search_key__owner = '', b.search_key__owner, a.search_key__owner) as owner,
                   if(a.search_key__repo = '', b.search_key__repo, a.search_key__repo)    as repo,
                   if(a.company = '', b.company, a.company)                               as company,
                   if(a.year = 0, b.year, a.year)                                         as year,
                   pr_count,
                   issues_count
            from (select search_key__owner,
                         search_key__repo,
                         year,
                         if(company = '', '其他', company) as company,
                         count()                           as pr_count
                  from (
                      select a.*, multiIf(user__login = 'jlebar' or user__login = 'Jokeren','openai',user__login='kshama-msft','microsoft',user__login='antiagainst','amd',user__login='daadaada','anthropic',b.company) as company
                        from (select search_key__owner,
                                     search_key__repo,
                                     id,
                                     user__login,
                                     pull_request__url,
                                     toYear(created_at) as year
                              from github_issues
                              where (search_key__owner = 'triton-lang')
                                and search_key__repo != '.github'
                                and pull_request__url != ''
                              group by search_key__owner, search_key__repo, id, pull_request__url,
                                       user__login, year) as a global
                                 left join (select author__login,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company
                                            from (select author__login, company
                                                  from commit_company
                                                  where (search_key__owner = 'triton-lang')
                                                    and search_key__repo != '.github'
                                                    and author__login != ''
                                                    and commit_company.company != ''
                                                  group by author__login, company
                                                  union all
                                                  select login,
                                                         if(final_company_inferred_from_company = 'facebook',
                                                            'meta',
                                                            final_company_inferred_from_company) as company
                                                  from github_profile
                                                  where github_profile.final_company_inferred_from_company != ''
                                                    and final_company_inferred_from_company != '无'
                                                  group by login, final_company_inferred_from_company)
                                            group by author__login, company
                                            order by author__login) as b on a.user__login = b.author__login)
                  group by search_key__owner, search_key__repo, company, year) as a global
                     full join (select search_key__owner,
                                       search_key__repo,
                                       year,
                                       if(company = '', '其他', company) as company,
                                       count()                           as issues_count
                                from (select a.*,  multiIf(user__login = 'jlebar','openai',user__login='antiagainst','amd',user__login='daadaada','anthropic',b.company) as company
                                      from (select search_key__owner,
                                                   search_key__repo,
                                                   id,
                                                   user__login,
                                                   pull_request__url,
                                                   toYear(created_at) as year
                                            from github_issues
                                            where (search_key__owner = 'triton-lang')
                                              and search_key__repo != '.github'
                                              and pull_request__url = ''
                                            group by search_key__owner, search_key__repo, id, pull_request__url,
                                                     user__login, year) as a global
                                               left join (select author__login,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company
                                                          from (select author__login, company
                                                                from commit_company
                                                                where (search_key__owner = 'triton-lang')
                                                                  and search_key__repo != '.github'
                                                                  and author__login != ''
                                                                  and commit_company.company != ''
                                                                group by author__login, company
                                                                union all
                                                                select login,
                                                                       if(final_company_inferred_from_company =
                                                                          'facebook',
                                                                          'meta',
                                                                          final_company_inferred_from_company) as company
                                                                from github_profile
                                                                where github_profile.final_company_inferred_from_company != ''
                                                                  and final_company_inferred_from_company != '无'
                                                                group by login, final_company_inferred_from_company)
                                                          group by author__login, company
                                                          order by author__login) as b
                                                         on a.user__login = b.author__login)
                                group by search_key__owner, search_key__repo, company, year) as b
                               on a.search_key__owner = b.search_key__owner and
                                  a.search_key__repo = b.search_key__repo and
                                  a.year = b.year and
                                  a.company = b.company
               ) as a global
               full join (select search_key__owner,
                                 search_key__repo,
                                 year,
                                 if(company = '', '其他', company) as company,
                                 count()                              issue_comment_count
                          from (select a.*,  multiIf(user__login = 'jlebar','openai',user__login='antiagainst','amd',user__login='daadaada','anthropic',b.company) as company
                                from (select search_key__owner,
                                             search_key__repo,
                                             id,
                                             user__login,
                                             toYear(created_at) as year
                                      from github_issues_comments
                                      where (search_key__owner = 'triton-lang')
                                        and search_key__repo != '.github'
                                      group by search_key__owner, search_key__repo, id, user__login, year) as a global
                                         left join (select author__login,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company
                                                    from (select author__login, company
                                                          from commit_company
                                                          where (search_key__owner = 'triton-lang')
                                                            and search_key__repo != '.github'
                                                            and author__login != ''
                                                            and commit_company.company != ''
                                                          group by author__login, company
                                                          union all
                                                          select login,
                                                                 if(final_company_inferred_from_company =
                                                                    'facebook',
                                                                    'meta',
                                                                    final_company_inferred_from_company) as company
                                                          from github_profile
                                                          where github_profile.final_company_inferred_from_company != ''
                                                            and final_company_inferred_from_company != '无'
                                                          group by login, final_company_inferred_from_company)
                                                    group by author__login, company
                                                    order by author__login) as b
                                                   on a.user__login = b.author__login)
                          group by search_key__owner, search_key__repo, company, year) as b
                         on a.owner = b.search_key__owner and a.repo = b.search_key__repo and a.year = b.year and
                            a.company = b.company) as a global
         full join (
    -- commit_count
    select search_key__owner,
           search_key__repo,
           year,
           company,
           commit_count,
           total_insertions,
           total_deletions,
           total_lines,
           developer_count
    from (select search_key__owner,
                 search_key__repo,
                 year,
                 if(company = '', '其他', company)                                         as company,
                 count(distinct hexsha)                                                    as commit_count,
                 sum(insertions)                                                           as total_insertions,
                 sum(deletions)                                                            as total_deletions,
                 sum(lines)                                                                as total_lines,
                 count(distinct lower(if(author__login = '', author_name, author__login))) as developer_count
          from (

--               , multiIf(user__login = 'jlebar','openai',user__login='antiagainst','amd',user__login='daadaada','anthropic',b.company) as company

              select * , multiIf(author__login = 'jlebar','openai',author__login='antiagainst','amd',author__login='daadaada','anthropic',company_) as company from (select search_key__owner,
                       search_key__repo,
                       hexsha,
                       author_name,
                       author_email,
                       parents,
                       insertions,
                       deletions,
                       lines,
                       year,
                       if(a.company = '', b.company, a.company) as company_,

                       author__login
                from (select search_key__owner,
                             search_key__repo,
                             hexsha,
                             author_name,
                             author_email,
                             parents,
                             insertions,
                             deletions,
                             lines,
                             year,
                             if(a.company = '', b.company, a.company) as company,
                             author__login
                      from (select a.*, b.company, b.author__login
                            from (select search_key__owner,
                                         search_key__repo,
                                         hexsha,
                                         author_name,
                                         author_email,
                                         parents,
                                         `files.insertions`       insertions,
                                         `files.deletions`        deletions,
                                         `files.lines`            lines,
                                         toYear(authored_date) as year
                                  from gits array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`
                                  where (search_key__owner = 'triton-lang')
                                    and search_key__repo != 'triton-cpu'
                                    and author_name != 'dependabot[bot]'
                                     ) as a global
                                     left join (select sha,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company, author__login
                                                from commit_company
                                                where (search_key__owner = 'triton-lang')
                                                  and search_key__repo != 'triton-cpu'
                                                group by sha, company, author__login) as b
                                               on a.hexsha = b.sha) as a global
                               left join (select * from company_email_map) as b
                                         on splitByChar('@', a.author_email)[2] = b.email_domain) as a global
                         left join (select search_key__owner,
                                           search_key__repo,
                                           author__login,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company,
                                           count() as commit_count
                                    from commit_company
                                    where author__login != ''
                                      and author__login not like '%[bot]%'
                                      and author__login != 'dependabot'
                                      and author__login not like '%-bot%'
                                      and author__login not like 'pytorchbot'
                                      and company != ''
                                    group by search_key__owner, search_key__repo, company, author__login
                                    order by commit_count desc
                                    limit 1 by search_key__owner,commit_company.search_key__repo,author__login) as b
                                   on a.search_key__owner = b.search_key__owner and
                                      a.search_key__repo = b.search_key__repo and
                                      a.author__login = b.author__login)

              )
          group by search_key__owner, search_key__repo, year, company
          order by year


          union all

          select search_key__owner,
                 search_key__repo,
                 year,
                 if(company = '', '其他', company)                                         as company,
                 count(distinct hexsha)                                                    as commit_count,
                 sum(insertions)                                                           as total_insertions,
                 sum(deletions)                                                            as total_deletions,
                 sum(lines)                                                                as total_lines,
                 count(distinct lower(if(author__login = '', author_name, author__login))) as developer_count
          from (
              select *,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company_) as company from (select search_key__owner,
                       search_key__repo,
                       hexsha,
                       author_name,
                       author_email,
                       parents,
                       insertions,
                       deletions,
                       lines,
                       year,
                       if(a.company = '', b.company, a.company) as company_,
                       author__login
                from (select search_key__owner,
                             search_key__repo,
                             hexsha,
                             author_name,
                             author_email,
                             parents,
                             insertions,
                             deletions,
                             lines,
                             year,
                             if(a.company = '', b.company, a.company) as company,
                             author__login
                      from (select a.*, b.company, b.author__login
                            from (select search_key__owner,
                                         search_key__repo,
                                         hexsha,
                                         author_name,
                                         author_email,
                                         parents,
                                         `files.insertions`       insertions,
                                         `files.deletions`        deletions,
                                         `files.lines`            lines,
                                         toYear(authored_date) as year
                                  from gits array join `files.file_name`, `files.insertions`, `files.deletions`, `files.lines`
                                  where (search_key__owner = 'triton-lang')
                                    and search_key__repo = 'triton-cpu'
                                    and hexsha global not in (select hexsha
                                                              from gits
                                                              where search_key__owner = 'triton-lang'
                                                                and search_key__repo = 'triton'
                                                              group by hexsha)
                                    and author_name != 'dependabot[bot]'
                                     ) as a global
                                     left join (select sha,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company, author__login
                                                from commit_company
                                                where (search_key__owner = 'triton-lang')
                                                  and search_key__repo = 'triton-cpu'
                                                group by sha, company, author__login) as b
                                               on a.hexsha = b.sha) as a global
                               left join (select * from company_email_map) as b
                                         on splitByChar('@', a.author_email)[2] = b.email_domain) as a global
                         left join (select search_key__owner,
                                           search_key__repo,
                                           author__login,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company,
                                           count() as commit_count
                                    from commit_company
                                    where author__login != ''
                                      and author__login not like '%[bot]%'
                                      and author__login != 'dependabot'
                                      and author__login not like '%-bot%'
                                      and author__login not like 'pytorchbot'
                                      and company != ''
                                    group by search_key__owner, search_key__repo, company, author__login
                                    order by commit_count desc
                                    limit 1 by search_key__owner,commit_company.search_key__repo,author__login) as b
                                   on a.search_key__owner = b.search_key__owner and
                                      a.search_key__repo = b.search_key__repo and
                                      a.author__login = b.author__login)
              )
          group by search_key__owner, search_key__repo, year, company
          order by year)
    ) as b
                   on a.owner = b.search_key__owner and a.repo = b.search_key__repo and a.company = b.company and
                      a.year = b.year
) as a
         global
         full join (select if(a.search_key__owner = '', b.search_key__owner, a.search_key__owner) as owner,
                           if(a.search_key__repo = '', b.search_key__repo, a.search_key__repo)    as repo,
                           if(a.company = '', b.company, a.company)                               as company,
                           if(a.year = 0, b.issue_comment_year, a.year)                           as year,
                           a.pr_merged_count,
                           a.pr_rejected_count,
                           b.commented_issues_count
                    from (-- 多少pr被合入 多少pr被拒绝
                             select search_key__owner,
                                    search_key__repo,
                                    if(company = '', '其他', company) as company,
                                    year,
                                    countIf(is_be_accepted = 'accepted') pr_merged_count,
                                    countIf(is_be_accepted = 'rejected') pr_rejected_count
                             from (select a.*, multiIf(user__login = 'jlebar','openai',user__login='antiagainst','amd',user__login='daadaada','anthropic',b.company) as company
                                   from (
                                            select search_key__owner,
                                                   search_key__repo,
                                                   id,
                                                   user__login,
                                                   pull_request__url,
                                                   toYear(created_at) as               year,
                                                   toYYYYMMDD(pull_request__merged_at) merged_at_day,
                                                   toYYYYMMDD(closed_at)               closed_at_day,
                                                   state,
                                                   multiIf(state = 'closed' and merged_at_day = 19700101, 'rejected',
                                                           state = 'closed' and merged_at_day != 19700101, 'accepted',
                                                           'open')                     is_be_accepted
                                            from github_issues
                                            where (search_key__owner = 'triton-lang')
                                              and search_key__repo != '.github'
                                              and pull_request__url != ''
                                            group by search_key__owner, search_key__repo, id, pull_request__url,
                                                     user__login, year, pull_request__merged_at, closed_at, state
                                            ) as a global
                                            left join (select author__login,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company
                                                       from (select author__login, company
                                                             from commit_company
                                                             where (search_key__owner = 'triton-lang')
                                                               and search_key__repo != '.github'
                                                               and author__login != ''
                                                               and commit_company.company != ''
                                                             group by author__login, company
                                                             union all
                                                             select login,
                                                                    if(final_company_inferred_from_company =
                                                                       'facebook',
                                                                       'meta',
                                                                       final_company_inferred_from_company) as company
                                                             from github_profile
                                                             where github_profile.final_company_inferred_from_company != ''
                                                               and final_company_inferred_from_company != '无'
                                                             group by login, final_company_inferred_from_company)
                                                       group by author__login, company
                                                       order by author__login) as b on a.user__login = b.author__login)
                             group by search_key__owner, search_key__repo, company, year) as a global
                             full join (-- 回复issues的个数
                        select search_key__owner,
                               search_key__repo,
                               company,
                               issue_comment_year,
                               count(distinct number) as commented_issues_count
                        from (select a.*,
                                     if(issues_comment_login = '', 'no', 'yes')                           as is_be_commented,
                                     if(b.company = '' and issues_comment_login != '', '其他', b.company) as company
                              from (select search_key__owner,
                                           search_key__repo,
                                           number,
                                           a.user__login as open_issue_user_login,
                                           a.year        as issue_year,
                                           closed_at_day,
                                           state,
                                           b.user__login as issues_comment_login,
                                           b.year        as issue_comment_year
                                    from (select search_key__owner,
                                                 search_key__repo,
                                                 number,
                                                 id,
                                                 user__login,
                                                 pull_request__url,
                                                 toYear(created_at) as year,
                                                 toYYYYMMDD(closed_at) closed_at_day,
                                                 state
                                          from github_issues
                                          where (search_key__owner = 'triton-lang')
                                            and search_key__repo != '.github'
                                            and pull_request__url = ''
                                          group by search_key__owner, search_key__repo, id, pull_request__url,
                                                   user__login, year, closed_at, state, number) as a global
                                             left join (select search_key__owner,
                                                               search_key__repo,
                                                               id,
                                                               user__login,
                                                               toYear(created_at) as year,
                                                               search_key__number
                                                        from github_issues_comments
                                                        where (search_key__owner = 'triton-lang')
                                                          and search_key__repo != '.github'
                                                        group by search_key__owner, search_key__repo, id, user__login,
                                                                 year,
                                                                 search_key__number
                                        ) as b on a.search_key__owner = b.search_key__owner and
                                                  a.search_key__repo = b.search_key__repo and
                                                  a.number = b.search_key__number
                                    where open_issue_user_login != issues_comment_login) as a global
                                       left join (select author__login,  multiIf(author__login = 'jlebar' or author__login = 'Jokeren','openai',author__login='kshama-msft','microsoft',author__login='antiagainst','amd',author__login='daadaada','anthropic',company) as company
                                                  from (select author__login, company
                                                        from commit_company
                                                        where (search_key__owner = 'triton-lang')
                                                          and search_key__repo != '.github'
                                                          and author__login != ''
                                                          and commit_company.company != ''
                                                        group by author__login, company
                                                        union all
                                                        select login,
                                                               if(final_company_inferred_from_company =
                                                                  'facebook',
                                                                  'meta',
                                                                  final_company_inferred_from_company) as company
                                                        from github_profile
                                                        where github_profile.final_company_inferred_from_company != ''
                                                          and final_company_inferred_from_company != '无'
                                                        group by login, final_company_inferred_from_company)
                                                  group by author__login, company
                                                  order by author__login) as b on
                                  a.issues_comment_login = b.author__login)
                        where is_be_commented = 'yes'
                        group by search_key__owner, search_key__repo, company, issue_comment_year) as b
                                       on a.search_key__owner = b.search_key__owner and
                                          a.search_key__repo = b.search_key__repo and
                                          a.company = b.company and a.year = b.issue_comment_year) as b
                   on a.owner = b.owner and a.repo = b.repo and a.company = b.company and a.year = b.year
-- where company!='Rice University'

order by owner, repo, issue_comment_count desc








-- 被拒绝被合入比例 按年份年份
select search_key__owner,
       search_key__repo,
       issue_year as year,
       countIf(is_be_commented_by_approver = 1) approver_comment_issues_count,
       countIf(is_be_commented_by_approver = 0) no_approver_comment_issues_count,
       round(no_approver_comment_issues_count/(approver_comment_issues_count+no_approver_comment_issues_count)*100,1) as no_approver_comment_issues_count_percentage
from (select search_key__owner, search_key__repo, number, is_be_commented_by_approver,issue_year
      from (select search_key__owner,
                   search_key__repo,
                   number,
                   open_issue_user_login,
                   issue_year,
                   closed_at_day,
                   state,
                   if(issues_comment_login != '', 1, 0) as is_be_commented_by_approver
            from (select search_key__owner,
                         search_key__repo,
                         number,
                         a.user__login as open_issue_user_login,
                         a.year        as issue_year,
                         closed_at_day,
                         state,
                         b.user__login as issues_comment_login,
                         b.year        as issue_comment_year
                  from (select a.*, if(b.login != '', 'yes', 'no') is_approver
                        from (select a.*,
                                     if(user__login global in
                                        ('galipremsagar', 'kkraus14', 'nithinraok', 'XuesongYang', 'codereport',
                                         'dillon-cullinan',
                                         'thomcom',
                                         'charlesbluca', 'jnke2016', 'naimnv'), 'nvidia', b.company) as company
                              from (
                                  -- all issues
                                  select search_key__owner,
                                           search_key__repo,
                                           number,
                                           id,
                                           user__login,
                                           pull_request__url,
                                           toYear(created_at) as year,
                                           toYYYYMMDD(closed_at) closed_at_day,
                                           state
                                    from github_issues
                                    where (search_key__owner = 'Project-MONAI' and search_key__repo = 'MONAI' or
                                           search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo' or
                                           search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo-Guardrails' or
                                           search_key__owner = 'onnx' and search_key__repo = 'onnx-tensorrt' or
                                           search_key__owner = 'NVIDIA' and
                                           search_key__repo = 'TransformerEngine' or
                                           search_key__owner = 'pytorch' and search_key__repo = 'TensorRT' or
                                           search_key__owner = 'NVIDIA-Omniverse' and search_key__repo = 'PhysX' or
                                           search_key__owner = 'PixarAnimationStudios' and
                                           search_key__repo = 'USD' or search_key__owner = 'NVIDIA-Omniverse' and
                                                                       search_key__repo = 'USD-proposals' or
                                           search_key__owner = 'NVIDIA' and search_key__repo = 'AMGX' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'rmm' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'raft' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'cuxfilter' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'cugraph' or
                                           search_key__owner = 'rapidsai' and search_key__repo = 'cudf' or
                                           search_key__owner = 'CVCUDA' and search_key__repo = 'CV-CUDA' or
                                           search_key__owner = 'NVIDIA' and search_key__repo = 'Megatron-LM')
                                      and pull_request__url = ''
                                      and user__login global not in ('GPUtester',
                                                                     'github-actions[bot]',
                                                                     'github-actions[bot]',
                                                                     'rapids-bot[bot]',
                                                                     'monai-bot',
                                                                     'dependabot[bot]',
                                                                     'deepsource-autofix[bot]',
                                                                     'pre-commit-ci[bot]',
                                                                     'probottest',
                                                                     'lgtm-com[bot]',
                                                                     'facebook-github-bot',
                                                                     'smartestrobotdai',
                                                                     'fbottau',
                                                                     'probottest')
                                    group by search_key__owner, search_key__repo, id, pull_request__url,
                                             user__login, year, closed_at, state, number) as a global
                                       left join (select author__login, company
                                                  from (select author__login, company
                                                        from commit_company
                                                        where (search_key__owner = 'Project-MONAI' and
                                                               search_key__repo = 'MONAI' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'NeMo' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'NeMo-Guardrails' or
                                                               search_key__owner = 'onnx' and
                                                               search_key__repo = 'onnx-tensorrt' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'TransformerEngine' or
                                                               search_key__owner = 'pytorch' and
                                                               search_key__repo = 'TensorRT' or
                                                               search_key__owner = 'NVIDIA-Omniverse' and
                                                               search_key__repo = 'PhysX' or
                                                               search_key__owner = 'PixarAnimationStudios' and
                                                               search_key__repo = 'USD' or
                                                               search_key__owner = 'NVIDIA-Omniverse' and
                                                               search_key__repo = 'USD-proposals' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'AMGX' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'rmm' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'raft' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'cuxfilter' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'cugraph' or
                                                               search_key__owner = 'rapidsai' and
                                                               search_key__repo = 'cudf' or
                                                               search_key__owner = 'CVCUDA' and
                                                               search_key__repo = 'CV-CUDA' or
                                                               search_key__owner = 'NVIDIA' and
                                                               search_key__repo = 'Megatron-LM')
                                                          and author__login != ''
                                                          and commit_company.company != ''
                                                        group by author__login, company
                                                        union all
                                                        select login,
                                                               if(final_company_inferred_from_company =
                                                                  'facebook',
                                                                  'meta',
                                                                  final_company_inferred_from_company) as company
                                                        from github_profile
                                                        where github_profile.final_company_inferred_from_company != ''
                                                          and final_company_inferred_from_company != '无'
                                                        group by login, final_company_inferred_from_company)
                                                  group by author__login, company
                                                  order by author__login) as b on a.user__login = b.author__login
                              where company != 'apache_org') as a global
                                 left join (select *
                                            from (select a.*,
                                                         if(login global in
                                                            ('galipremsagar', 'kkraus14', 'nithinraok', 'XuesongYang',
                                                             'codereport',
                                                             'dillon-cullinan', 'thomcom',
                                                             'charlesbluca', 'jnke2016', 'naimnv'), 'nvidia',
                                                            b.company) as company
                                                  from (select search_key__owner,
                                                               search_key__repo,
                                                               login,
                                                               count() as approved_count
                                                        from (select search_key__owner,
                                                                     search_key__repo,
                                                                     JSONExtractString(timeline_raw, 'id')                               as id,
                                                                     JSONExtractString(timeline_raw, 'state')                            as state,
                                                                     JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'login') as login
                                                              from github_issues_timeline
                                                              where search_key__event = 'reviewed'
                                                                and (search_key__owner = 'Project-MONAI' and
                                                                     search_key__repo = 'MONAI' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'NeMo' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'NeMo-Guardrails' or
                                                                     search_key__owner = 'onnx' and
                                                                     search_key__repo = 'onnx-tensorrt' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'TransformerEngine' or
                                                                     search_key__owner = 'pytorch' and
                                                                     search_key__repo = 'TensorRT' or
                                                                     search_key__owner = 'NVIDIA-Omniverse' and
                                                                     search_key__repo = 'PhysX' or
                                                                     search_key__owner = 'PixarAnimationStudios' and
                                                                     search_key__repo = 'USD' or
                                                                     search_key__owner = 'NVIDIA-Omniverse' and
                                                                     search_key__repo = 'USD-proposals' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'AMGX' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'rmm' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'raft' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'cuxfilter' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'cugraph' or
                                                                     search_key__owner = 'rapidsai' and
                                                                     search_key__repo = 'cudf' or
                                                                     search_key__owner = 'CVCUDA' and
                                                                     search_key__repo = 'CV-CUDA' or
                                                                     search_key__owner = 'NVIDIA' and
                                                                     search_key__repo = 'Megatron-LM')
                                                                and (state = 'approved' or state = 'changes_requested' or state = 'dismissed')
                                                                and login global not in ('GPUtester',
                                                                                         'github-actions[bot]',
                                                                                         'github-actions[bot]',
                                                                                         'rapids-bot[bot]',
                                                                                         'monai-bot',
                                                                                         'dependabot[bot]',
                                                                                         'deepsource-autofix[bot]',
                                                                                         'pre-commit-ci[bot]',
                                                                                         'probottest',
                                                                                         'lgtm-com[bot]',
                                                                                         'facebook-github-bot',
                                                                                         'smartestrobotdai',
                                                                                         'fbottau',
                                                                                         'probottest')
                                                              group by search_key__owner, search_key__repo, id, state, login)
                                                        group by search_key__owner, search_key__repo, login
                                                        having approved_count > 10
                                                        order by search_key__owner, search_key__repo, approved_count desc
                                                           ) as a global
                                                           left join (select *
                                                                      from (select author__login, company
                                                                            from (select author__login, company
                                                                                  from commit_company
                                                                                  where (search_key__owner =
                                                                                         'Project-MONAI' and
                                                                                         search_key__repo = 'MONAI' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo = 'NeMo' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo =
                                                                                         'NeMo-Guardrails' or
                                                                                         search_key__owner = 'onnx' and
                                                                                         search_key__repo =
                                                                                         'onnx-tensorrt' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo =
                                                                                         'TransformerEngine' or
                                                                                         search_key__owner =
                                                                                         'pytorch' and
                                                                                         search_key__repo =
                                                                                         'TensorRT' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA-Omniverse' and
                                                                                         search_key__repo = 'PhysX' or
                                                                                         search_key__owner =
                                                                                         'PixarAnimationStudios' and
                                                                                         search_key__repo = 'USD' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA-Omniverse' and
                                                                                         search_key__repo =
                                                                                         'USD-proposals' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo = 'AMGX' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo = 'rmm' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo = 'raft' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo =
                                                                                         'cuxfilter' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo = 'cugraph' or
                                                                                         search_key__owner =
                                                                                         'rapidsai' and
                                                                                         search_key__repo = 'cudf' or
                                                                                         search_key__owner =
                                                                                         'CVCUDA' and
                                                                                         search_key__repo = 'CV-CUDA' or
                                                                                         search_key__owner =
                                                                                         'NVIDIA' and
                                                                                         search_key__repo =
                                                                                         'Megatron-LM')
                                                                                    and author__login != ''
                                                                                    and commit_company.company != ''
                                                                                  group by author__login, company
                                                                                  union all
                                                                                  select login,
                                                                                         if(
                                                                                                 final_company_inferred_from_company =
                                                                                                 'facebook',
                                                                                                 'meta',
                                                                                                 final_company_inferred_from_company) as company
                                                                                  from github_profile
                                                                                  where github_profile.final_company_inferred_from_company != ''
                                                                                    and final_company_inferred_from_company != '无'
                                                                                  group by login, final_company_inferred_from_company)
                                                                            group by author__login, company
                                                                            order by author__login)
                                                                      where author__login != '') as b
                                                                     on a.login = b.author__login
                                                  order by search_key__owner, search_key__repo, approved_count desc
                                                  limit 30 by search_key__repo)
                                            where company = 'nvidia') as b
                                           on a.search_key__owner = b.search_key__owner and
                                              a.search_key__repo = b.search_key__repo and
                                              a.user__login = b.login
                        where company != 'nvidia'
                          and is_approver = 'no') as a global
                           left join (select *
                                      from (select search_key__owner,
                                                   search_key__repo,
                                                   id,
                                                   user__login,
                                                   toYear(created_at) as year,
                                                   search_key__number
                                            from github_issues_comments
                                            where (search_key__owner = 'Project-MONAI' and search_key__repo = 'MONAI' or
                                                   search_key__owner = 'NVIDIA' and search_key__repo = 'NeMo' or
                                                   search_key__owner = 'NVIDIA' and
                                                   search_key__repo = 'NeMo-Guardrails' or
                                                   search_key__owner = 'onnx' and search_key__repo = 'onnx-tensorrt' or
                                                   search_key__owner = 'NVIDIA' and
                                                   search_key__repo = 'TransformerEngine' or
                                                   search_key__owner = 'pytorch' and search_key__repo = 'TensorRT' or
                                                   search_key__owner = 'NVIDIA-Omniverse' and
                                                   search_key__repo = 'PhysX' or
                                                   search_key__owner = 'PixarAnimationStudios' and
                                                   search_key__repo = 'USD' or
                                                   search_key__owner = 'NVIDIA-Omniverse' and
                                                   search_key__repo = 'USD-proposals' or
                                                   search_key__owner = 'NVIDIA' and search_key__repo = 'AMGX' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'rmm' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'raft' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'cuxfilter' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'cugraph' or
                                                   search_key__owner = 'rapidsai' and search_key__repo = 'cudf' or
                                                   search_key__owner = 'CVCUDA' and search_key__repo = 'CV-CUDA' or
                                                   search_key__owner = 'NVIDIA' and search_key__repo = 'Megatron-LM')
                                              and user__login global not in ('GPUtester',
                                                                             'github-actions[bot]',
                                                                             'github-actions[bot]',
                                                                             'rapids-bot[bot]',
                                                                             'monai-bot',
                                                                             'dependabot[bot]',
                                                                             'deepsource-autofix[bot]',
                                                                             'pre-commit-ci[bot]',
                                                                             'probottest',
                                                                             'lgtm-com[bot]',
                                                                             'facebook-github-bot',
                                                                             'smartestrobotdai',
                                                                             'fbottau',
                                                                             'probottest')
                                            group by search_key__owner, search_key__repo, id, user__login,
                                                     year,
                                                     search_key__number) as a global
                                               join (select *
                                                     from (select a.*,
                                                                  if(login global in
                                                                     ('galipremsagar', 'kkraus14', 'nithinraok',
                                                                      'XuesongYang',
                                                                      'codereport',
                                                                      'dillon-cullinan', 'thomcom',
                                                                      'charlesbluca', 'jnke2016', 'naimnv'), 'nvidia',
                                                                     b.company) as company
                                                           from (select search_key__owner,
                                                                        search_key__repo,
                                                                        login,
                                                                        count() as approved_count
                                                                 from (select search_key__owner,
                                                                              search_key__repo,
                                                                              JSONExtractString(timeline_raw, 'id')                               as id,
                                                                              JSONExtractString(timeline_raw, 'state')                            as state,
                                                                              JSONExtractString(JSONExtractString(timeline_raw, 'user'), 'login') as login
                                                                       from github_issues_timeline
                                                                       where search_key__event = 'reviewed'
                                                                         and (search_key__owner = 'Project-MONAI' and
                                                                              search_key__repo = 'MONAI' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'NeMo' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'NeMo-Guardrails' or
                                                                              search_key__owner = 'onnx' and
                                                                              search_key__repo = 'onnx-tensorrt' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'TransformerEngine' or
                                                                              search_key__owner = 'pytorch' and
                                                                              search_key__repo = 'TensorRT' or
                                                                              search_key__owner = 'NVIDIA-Omniverse' and
                                                                              search_key__repo = 'PhysX' or
                                                                              search_key__owner =
                                                                              'PixarAnimationStudios' and
                                                                              search_key__repo = 'USD' or
                                                                              search_key__owner = 'NVIDIA-Omniverse' and
                                                                              search_key__repo = 'USD-proposals' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'AMGX' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'rmm' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'raft' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'cuxfilter' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'cugraph' or
                                                                              search_key__owner = 'rapidsai' and
                                                                              search_key__repo = 'cudf' or
                                                                              search_key__owner = 'CVCUDA' and
                                                                              search_key__repo = 'CV-CUDA' or
                                                                              search_key__owner = 'NVIDIA' and
                                                                              search_key__repo = 'Megatron-LM')
                                                                         and (state = 'approved' or state = 'changes_requested' or state = 'dismissed')
                                                                         and login global not in ('GPUtester',
                                                                                                  'github-actions[bot]',
                                                                                                  'github-actions[bot]',
                                                                                                  'rapids-bot[bot]',
                                                                                                  'monai-bot',
                                                                                                  'dependabot[bot]',
                                                                                                  'deepsource-autofix[bot]',
                                                                                                  'pre-commit-ci[bot]',
                                                                                                  'probottest',
                                                                                                  'lgtm-com[bot]',
                                                                                                  'facebook-github-bot',
                                                                                                  'smartestrobotdai',
                                                                                                  'fbottau',
                                                                                                  'probottest')
                                                                       group by search_key__owner, search_key__repo, id, state, login)
                                                                 group by search_key__owner, search_key__repo, login
                                                                 having approved_count > 10
                                                                 order by search_key__owner, search_key__repo, approved_count desc
                                                                    ) as a global
                                                                    left join (select *
                                                                               from (select author__login, company
                                                                                     from (select author__login, company
                                                                                           from commit_company
                                                                                           where (search_key__owner =
                                                                                                  'Project-MONAI' and
                                                                                                  search_key__repo =
                                                                                                  'MONAI' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'NeMo' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'NeMo-Guardrails' or
                                                                                                  search_key__owner =
                                                                                                  'onnx' and
                                                                                                  search_key__repo =
                                                                                                  'onnx-tensorrt' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'TransformerEngine' or
                                                                                                  search_key__owner =
                                                                                                  'pytorch' and
                                                                                                  search_key__repo =
                                                                                                  'TensorRT' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA-Omniverse' and
                                                                                                  search_key__repo =
                                                                                                  'PhysX' or
                                                                                                  search_key__owner =
                                                                                                  'PixarAnimationStudios' and
                                                                                                  search_key__repo =
                                                                                                  'USD' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA-Omniverse' and
                                                                                                  search_key__repo =
                                                                                                  'USD-proposals' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'AMGX' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'rmm' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'raft' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'cuxfilter' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'cugraph' or
                                                                                                  search_key__owner =
                                                                                                  'rapidsai' and
                                                                                                  search_key__repo =
                                                                                                  'cudf' or
                                                                                                  search_key__owner =
                                                                                                  'CVCUDA' and
                                                                                                  search_key__repo =
                                                                                                  'CV-CUDA' or
                                                                                                  search_key__owner =
                                                                                                  'NVIDIA' and
                                                                                                  search_key__repo =
                                                                                                  'Megatron-LM')
                                                                                             and author__login != ''
                                                                                             and commit_company.company != ''
                                                                                           group by author__login, company
                                                                                           union all
                                                                                           select login,
                                                                                                  if(
                                                                                                          final_company_inferred_from_company =
                                                                                                          'facebook',
                                                                                                          'meta',
                                                                                                          final_company_inferred_from_company) as company
                                                                                           from github_profile
                                                                                           where github_profile.final_company_inferred_from_company != ''
                                                                                             and final_company_inferred_from_company != '无'
                                                                                           group by login, final_company_inferred_from_company)
                                                                                     group by author__login, company
                                                                                     order by author__login)
                                                                               where author__login != '') as b
                                                                              on a.login = b.author__login
                                                           order by search_key__owner, search_key__repo, approved_count desc
                                                           limit 30 by search_key__repo)
                                                     where company = 'nvidia') as b
                                                    on a.search_key__owner = b.search_key__owner and
                                                       a.search_key__repo = b.search_key__repo and
                                                       a.user__login = b.login
                      ) as b on a.search_key__owner = b.search_key__owner and
                                a.search_key__repo = b.search_key__repo and
                                a.number = b.search_key__number
                  where open_issue_user_login != issues_comment_login))
      group by search_key__owner, search_key__repo, number, is_be_commented_by_approver,issue_year)
group by search_key__owner, search_key__repo,issue_year
